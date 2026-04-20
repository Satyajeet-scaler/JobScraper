#!/usr/bin/env python3
"""
Detect likely Cloudflare challenge click targets from a screenshot.

This script is image-driven (no hardcoded absolute coordinates).
It outputs:
- Annotated PNG with marker points.
- JSON with widget box and candidate click coordinates.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

from PIL import Image, ImageDraw, ImageFont


BBox = tuple[int, int, int, int]


def _luma(r: int, g: int, b: int) -> int:
    return (299 * r + 587 * g + 114 * b) // 1000


def _is_dark_neutral(r: int, g: int, b: int) -> bool:
    lum = _luma(r, g, b)
    return (
        lum < 105
        and abs(r - g) < 45
        and abs(r - b) < 45
        and abs(g - b) < 45
    )


def _bbox_from_predicate(
    image: Image.Image,
    predicate: Callable[[int, int, int, int, int], bool],
    *,
    stride: int = 1,
) -> tuple[BBox | None, int]:
    width, height = image.size
    pixels = image.load()

    min_x, min_y = width, height
    max_x, max_y = -1, -1
    count = 0

    for y in range(0, height, stride):
        for x in range(0, width, stride):
            r, g, b = pixels[x, y][:3]
            if predicate(x, y, r, g, b):
                count += 1
                if x < min_x:
                    min_x = x
                if y < min_y:
                    min_y = y
                if x > max_x:
                    max_x = x
                if y > max_y:
                    max_y = y

    if count == 0:
        return None, 0

    return (min_x, min_y, max_x, max_y), count


def _expand_and_clip(box: BBox, *, pad: int, width: int, height: int) -> BBox:
    x1, y1, x2, y2 = box
    return (
        max(0, x1 - pad),
        max(0, y1 - pad),
        min(width - 1, x2 + pad),
        min(height - 1, y2 + pad),
    )


def _find_checkbox_component(image: Image.Image) -> tuple[BBox | None, dict[str, Any]]:
    width, height = image.size
    pixels = image.load()

    sx1 = int(width * 0.20)
    sx2 = int(width * 0.62)
    sy1 = int(height * 0.18)
    sy2 = int(height * 0.58)

    region_w = sx2 - sx1 + 1
    region_h = sy2 - sy1 + 1
    visited = bytearray(region_w * region_h)

    def idx(x: int, y: int) -> int:
        return (y - sy1) * region_w + (x - sx1)

    best_box: BBox | None = None
    best_score = float("-inf")
    best_meta: dict[str, Any] = {}

    for y in range(sy1, sy2 + 1):
        for x in range(sx1, sx2 + 1):
            i = idx(x, y)
            if visited[i]:
                continue

            r, g, b = pixels[x, y][:3]
            if not _is_dark_neutral(r, g, b):
                visited[i] = 1
                continue

            stack = [(x, y)]
            visited[i] = 1
            count = 0
            min_x = max_x = x
            min_y = max_y = y

            while stack:
                cx, cy = stack.pop()
                count += 1
                if cx < min_x:
                    min_x = cx
                if cx > max_x:
                    max_x = cx
                if cy < min_y:
                    min_y = cy
                if cy > max_y:
                    max_y = cy

                for nx, ny in ((cx - 1, cy), (cx + 1, cy), (cx, cy - 1), (cx, cy + 1)):
                    if nx < sx1 or nx > sx2 or ny < sy1 or ny > sy2:
                        continue
                    ni = idx(nx, ny)
                    if visited[ni]:
                        continue
                    nr, ng, nb = pixels[nx, ny][:3]
                    if _is_dark_neutral(nr, ng, nb):
                        visited[ni] = 1
                        stack.append((nx, ny))
                    else:
                        visited[ni] = 1

            comp_w = max_x - min_x + 1
            comp_h = max_y - min_y + 1
            area = comp_w * comp_h
            if area <= 0:
                continue

            aspect = comp_w / max(1, comp_h)
            fill = count / area

            if not (12 <= comp_w <= 42 and 12 <= comp_h <= 42):
                continue
            if not (0.72 <= aspect <= 1.35):
                continue
            if not (0.14 <= fill <= 0.78):
                continue

            inner_x1 = min_x + 2
            inner_y1 = min_y + 2
            inner_x2 = max_x - 2
            inner_y2 = max_y - 2
            if inner_x2 <= inner_x1 or inner_y2 <= inner_y1:
                continue

            inner_count = 0
            inner_dark = 0
            inner_luma_sum = 0
            for iy in range(inner_y1, inner_y2 + 1):
                for ix in range(inner_x1, inner_x2 + 1):
                    ir, ig, ib = pixels[ix, iy][:3]
                    inner_count += 1
                    inner_luma_sum += _luma(ir, ig, ib)
                    if _is_dark_neutral(ir, ig, ib):
                        inner_dark += 1
            if inner_count == 0:
                continue

            inner_luma_avg = inner_luma_sum / inner_count
            inner_dark_ratio = inner_dark / inner_count
            if inner_luma_avg < 120 or inner_dark_ratio > 0.30:
                continue

            border_count = 0
            border_dark = 0
            for ix in range(min_x, max_x + 1):
                for iy in (min_y, max_y):
                    br, bg, bb = pixels[ix, iy][:3]
                    border_count += 1
                    if _is_dark_neutral(br, bg, bb):
                        border_dark += 1
            for iy in range(min_y + 1, max_y):
                for ix in (min_x, max_x):
                    br, bg, bb = pixels[ix, iy][:3]
                    border_count += 1
                    if _is_dark_neutral(br, bg, bb):
                        border_dark += 1

            border_dark_ratio = border_dark / max(1, border_count)
            if border_dark_ratio < 0.28:
                continue

            size_penalty = abs(comp_w - 20) + abs(comp_h - 20)
            center_x = (min_x + max_x) / 2.0
            left_bias = max(0.0, 1.0 - ((center_x - sx1) / max(1, (sx2 - sx1))))
            score = (
                border_dark_ratio * 2.5
                + (inner_luma_avg / 255.0)
                + (left_bias * 0.8)
                - (size_penalty * 0.02)
            )

            if score > best_score:
                best_score = score
                best_box = (min_x, min_y, max_x, max_y)
                best_meta = {
                    "score": round(score, 4),
                    "component_w": comp_w,
                    "component_h": comp_h,
                    "fill": round(fill, 4),
                    "inner_luma_avg": round(inner_luma_avg, 2),
                    "inner_dark_ratio": round(inner_dark_ratio, 4),
                    "border_dark_ratio": round(border_dark_ratio, 4),
                }

    info = {
        "search_region": {"x1": sx1, "y1": sy1, "x2": sx2, "y2": sy2},
        "best_score": (round(best_score, 4) if best_box else None),
        "best_meta": best_meta,
    }
    return best_box, info


def _estimate_widget_box(image: Image.Image) -> tuple[BBox, dict[str, Any]]:
    width, height = image.size

    checkbox_box, checkbox_info = _find_checkbox_component(image)

    green_box, green_count = _bbox_from_predicate(
        image,
        lambda _x, _y, r, g, b: g > 95 and (g - r) > 22 and (g - b) > 10,
        stride=1,
    )

    orange_box, orange_count = _bbox_from_predicate(
        image,
        lambda _x, _y, r, g, b: r > 180 and g > 95 and b < 120 and (r - b) > 70,
        stride=1,
    )

    dark_text_box, dark_count = _bbox_from_predicate(
        image,
        lambda _x, y, r, g, b: y < int(height * 0.82) and ((r + g + b) // 3) < 70,
        stride=2,
    )

    detection_method = "fallback"

    if checkbox_box:
        cx1, cy1, cx2, cy2 = checkbox_box
        raw = (cx1 - 10, cy1 - 14, cx2 + 260, cy2 + 14)
        widget_box = _expand_and_clip(raw, pad=0, width=width, height=height)
        detection_method = "checkbox_component"
    elif green_box and orange_box and green_count > 20 and orange_count > 20:
        gx1, gy1, gx2, gy2 = green_box
        ox1, oy1, ox2, oy2 = orange_box
        raw = (min(gx1, ox1), min(gy1, oy1), max(gx2, ox2), max(gy2, oy2))
        widget_box = _expand_and_clip(raw, pad=22, width=width, height=height)
        detection_method = "green_orange_cluster"
    elif green_box and green_count > 20:
        gx1, gy1, gx2, gy2 = green_box
        raw = (gx1 - 18, gy1 - 18, gx2 + 260, gy2 + 34)
        widget_box = _expand_and_clip(raw, pad=0, width=width, height=height)
        detection_method = "green_cluster"
    elif dark_text_box and dark_count > 40:
        tx1, _ty1, _tx2, ty2 = dark_text_box
        approx_width = int(width * 0.22)
        approx_height = int(height * 0.065)
        anchor_y = max(0, ty2 - int(height * 0.085))
        raw = (tx1, anchor_y, tx1 + approx_width, anchor_y + approx_height)
        widget_box = _expand_and_clip(raw, pad=0, width=width, height=height)
        detection_method = "text_anchor_fallback"
    else:
        cx = int(width * 0.31)
        cy = int(height * 0.34)
        half_w = int(width * 0.08)
        half_h = int(height * 0.03)
        raw = (cx - half_w, cy - half_h, cx + half_w, cy + half_h)
        widget_box = _expand_and_clip(raw, pad=0, width=width, height=height)
        detection_method = "center_fallback"

    info = {
        "method": detection_method,
        "green_box": green_box,
        "green_count": green_count,
        "orange_box": orange_box,
        "orange_count": orange_count,
        "dark_text_box": dark_text_box,
        "dark_count": dark_count,
        "checkbox_box": checkbox_box,
        "checkbox_info": checkbox_info,
    }
    return widget_box, info


def _candidate_points(widget_box: BBox, checkbox_box: BBox | None = None) -> list[dict[str, Any]]:
    if checkbox_box:
        x1, y1, x2, y2 = checkbox_box
        cx = int((x1 + x2) / 2)
        cy = int((y1 + y2) / 2)
        w = max(1, x2 - x1)
        h = max(1, y2 - y1)
        inset_x = max(2, int(w * 0.22))
        inset_y = max(2, int(h * 0.22))
        points = [
            (cx, cy),
            (int(x1 + inset_x), cy),
            (int(x2 - inset_x), cy),
            (cx, int(y1 + inset_y)),
        ]
        return [
            {
                "rank": i + 1,
                "x": int(pt[0]),
                "y": int(pt[1]),
                "label": f"P{i + 1}",
                "recommended": i == 0,
            }
            for i, pt in enumerate(points)
        ]

    x1, y1, x2, y2 = widget_box
    w = max(1, x2 - x1)
    h = max(1, y2 - y1)

    p1 = (int(x1 + 0.20 * w), int(y1 + 0.50 * h))
    p2 = (int(x1 + 0.32 * w), int(y1 + 0.50 * h))
    p3 = (int(x1 + 0.50 * w), int(y1 + 0.50 * h))
    p4 = (int(x1 + 0.20 * w), int(y1 + 0.68 * h))

    points = [p1, p2, p3, p4]
    return [
        {
            "rank": i + 1,
            "x": int(pt[0]),
            "y": int(pt[1]),
            "label": f"P{i + 1}",
            "recommended": i == 0,
        }
        for i, pt in enumerate(points)
    ]


def _draw_annotations(
    image: Image.Image,
    widget_box: BBox,
    points: list[dict[str, Any]],
    detection_method: str,
    checkbox_box: BBox | None = None,
) -> Image.Image:
    annotated = image.copy().convert("RGB")
    draw = ImageDraw.Draw(annotated)
    font = ImageFont.load_default()

    x1, y1, x2, y2 = widget_box
    draw.rectangle((x1, y1, x2, y2), outline=(255, 0, 0), width=3)
    draw.text((x1, max(0, y1 - 14)), f"widget_box ({detection_method})", fill=(255, 0, 0), font=font)

    if checkbox_box:
        cx1, cy1, cx2, cy2 = checkbox_box
        draw.rectangle((cx1, cy1, cx2, cy2), outline=(0, 180, 255), width=3)
        draw.text((cx1, max(0, cy1 - 14)), "checkbox_anchor", fill=(0, 180, 255), font=font)

    for point in points:
        px = point["x"]
        py = point["y"]
        rank = point["rank"]
        color = (255, 0, 0) if point["recommended"] else (255, 140, 0)
        radius = 12 if point["recommended"] else 9

        draw.ellipse((px - radius, py - radius, px + radius, py + radius), outline=color, width=3)
        draw.text((px + 12, py - 12), f"P{rank} ({px},{py})", fill=color, font=font)

    return annotated


def _default_out_image(in_path: Path) -> Path:
    return in_path.with_name(f"{in_path.stem}_clickmap.png")


def _default_out_json(in_path: Path) -> Path:
    return in_path.with_name(f"{in_path.stem}_clickmap.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Find dynamic Cloudflare click candidates from screenshot.")
    parser.add_argument("--image", required=True, help="Input screenshot PNG path")
    parser.add_argument("--out-image", help="Annotated output image path")
    parser.add_argument("--out-json", help="JSON output path")
    parser.add_argument(
        "--all-candidates",
        action="store_true",
        help="Keep all candidate points in output (default keeps only rank-1 click).",
    )
    args = parser.parse_args()

    in_path = Path(args.image).expanduser().resolve()
    out_image = Path(args.out_image).expanduser().resolve() if args.out_image else _default_out_image(in_path)
    out_json = Path(args.out_json).expanduser().resolve() if args.out_json else _default_out_json(in_path)

    image = Image.open(in_path)

    widget_box, info = _estimate_widget_box(image)
    checkbox_box = info.get("checkbox_box")
    points = _candidate_points(widget_box, checkbox_box=checkbox_box)
    if not args.all_candidates and points:
        points = [points[0]]

    annotated = _draw_annotations(
        image,
        widget_box,
        points,
        info["method"],
        checkbox_box=checkbox_box,
    )
    out_image.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    annotated.save(out_image)

    payload = {
        "image": str(in_path),
        "image_size": {"width": image.size[0], "height": image.size[1]},
        "widget_box": {
            "x1": widget_box[0],
            "y1": widget_box[1],
            "x2": widget_box[2],
            "y2": widget_box[3],
            "width": widget_box[2] - widget_box[0],
            "height": widget_box[3] - widget_box[1],
        },
        "click_candidates": points,
        "single_click_mode": (not args.all_candidates),
        "detection_info": info,
        "annotated_image": str(out_image),
    }

    out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str) + "\n", encoding="utf-8")

    print(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
