"""
Hiring.cafe grid: each company card can show multiple jobs (last 24h) behind a small
carousel — pagination dots + **next** (right chevron) / **prev** buttons.

This module expands each visible card’s carousel by clicking **next** until no new
``/viewjob/`` links appear in that card (or the button is disabled / max clicks).

Env:
  HIRECAFE_CAROUSEL_MAX_CLICKS_PER_CARD — default 40
  HIRECAFE_CAROUSEL_CLICK_PAUSE — base seconds to wait after each **next** click (default 0.35)
  HIRECAFE_CAROUSEL_EXTRA_PAUSE_AFTER_CLICK — extra seconds after that wait (default 0.15) so the next job slide can load
  HIRECAFE_VIEWPORT_FILTER_CHUNK — batch size for ``getBoundingClientRect`` viewport filter (default 80)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

# Public shape for ``expand_all_visible_company_cards_detailed`` rows.
CardJobsRow = dict[str, Any]

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

logger = logging.getLogger(__name__)

MAX_CLICKS = max(5, int(os.getenv("HIRECAFE_CAROUSEL_MAX_CLICKS_PER_CARD", "40")))
CLICK_PAUSE = float(os.getenv("HIRECAFE_CAROUSEL_CLICK_PAUSE", "0.35"))
# Extra wait after carousel "next" so the following job row / network payloads can settle.
EXTRA_PAUSE_AFTER_NEXT_CLICK = float(os.getenv("HIRECAFE_CAROUSEL_EXTRA_PAUSE_AFTER_CLICK", "0.15"))


def _pause_after_carousel_next_click() -> None:
    time.sleep(max(0.0, CLICK_PAUSE + EXTRA_PAUSE_AFTER_NEXT_CLICK))


def _infinite_scroll_root(driver: Any) -> WebElement | None:
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "div.infinite-scroll-component")
        if not els:
            return None
        for el in els:
            try:
                oy = driver.execute_script(
                    "return window.getComputedStyle(arguments[0]).overflowY",
                    el,
                )
                if oy in ("auto", "scroll"):
                    return el
            except Exception:
                continue
        return els[0]
    except Exception:
        return None


def _grid_children_cards(scroll_root: WebElement) -> list[WebElement]:
    """Direct children of the main job grid inside the feed (one column cell per card)."""
    try:
        grids = scroll_root.find_elements(By.CSS_SELECTOR, "div[class*='grid']")
        if not grids:
            return []
        # Outermost grid that lists cards (often grid-cols-*)
        grid = grids[0]
        children = grid.find_elements(By.XPATH, "./div")
        if children:
            return children
    except Exception:
        pass
    # Fallback: column cells that contain a viewjob link (typical card wrapper)
    try:
        return scroll_root.find_elements(
            By.XPATH,
            ".//div[contains(@class,'relative')][.//a[contains(@href,'/viewjob/')]]",
        )
    except Exception:
        return []


def _viewjob_hrefs_in_card(driver: Any, card: WebElement) -> set[str]:
    try:
        hrefs = driver.execute_script(
            """
            const c = arguments[0];
            const out = new Set();
            for (const a of c.querySelectorAll('a[href*="/viewjob/"]')) {
              const h = (a.getAttribute('href') || '').trim();
              if (h) out.add(h);
            }
            return Array.from(out);
            """,
            card,
        )
        return {str(h) for h in (hrefs or [])}
    except Exception:
        return set()


def _click_carousel_next(driver: Any, card: WebElement) -> bool:
    """
    Click the carousel **next** control (typically the rightmost button with chevron).
    Returns False if no control or already at end / disabled.
    """
    try:
        return bool(
            driver.execute_script(
                """
                const card = arguments[0];
                const navCandidates = card.querySelectorAll(
                  'div.flex.justify-center, div[class*="justify-center"]'
                );
                let nav = null;
                for (const n of navCandidates) {
                  const btns = n.querySelectorAll('button');
                  if (btns.length >= 1) {
                    nav = n;
                    break;
                  }
                }
                if (!nav) {
                  const all = card.querySelectorAll('div.flex.items-center button, div.flex button');
                  if (all.length >= 2) {
                    const parent = all[all.length - 1].parentElement;
                    if (parent) nav = parent;
                  }
                }
                if (!nav) return false;
                const btns = [...nav.querySelectorAll('button')];
                if (btns.length === 0) return false;
                const nextBtn = btns[btns.length - 1];
                if (nextBtn.disabled) return false;
                if (nextBtn.getAttribute('aria-disabled') === 'true') return false;
                const style = window.getComputedStyle(nextBtn);
                if (style.visibility === 'hidden' || style.display === 'none') return false;
                nextBtn.click();
                return true;
                """,
                card,
            )
        )
    except Exception as exc:
        logger.debug("hirecafe carousel next click failed: %s", type(exc).__name__)
        return False


def expand_carousels_in_card(driver: Any, card: WebElement) -> set[str]:
    """
    Click **next** on this company card until the next button is gone/disabled or
    no new ``/viewjob/`` hrefs appear after a click (carousel wrapped or single job).
    """
    collected: set[str] = set()
    for i in range(MAX_CLICKS):
        collected |= _viewjob_hrefs_in_card(driver, card)
        if not _click_carousel_next(driver, card):
            break
        _pause_after_carousel_next_click()
        new_batch = _viewjob_hrefs_in_card(driver, card)
        if i > 0 and not (new_batch - collected):
            break
        collected |= new_batch

    return collected


def _structural_dedupe_card_indices(driver: Any, cards: list[WebElement]) -> list[int] | None:
    """
    Indices of card nodes that are **not** strict descendants of another node in the list.

    Uses one browser round-trip for N>1. The previous O(n²) Python loop caused
    multi-minute stalls (no logs, no clicks) on ~200+ grid cells.
    """
    if len(cards) <= 1:
        return list(range(len(cards)))
    try:
        indices = driver.execute_script(
            """
            const nodes = arguments;
            const n = nodes.length;
            const out = [];
            for (let i = 0; i < n; i++) {
              let inner = false;
              for (let j = 0; j < n; j++) {
                if (i === j) continue;
                try {
                  if (nodes[j] !== nodes[i] && nodes[j].contains(nodes[i])) {
                    inner = true;
                    break;
                  }
                } catch (e) {}
              }
              if (!inner) out.push(i);
            }
            return out;
            """,
            *cards,
        )
        if not indices:
            return None
        return [int(i) for i in indices]
    except Exception:
        return None


def _visible_job_dedupe_keys_for_cards(driver: Any, cards: list[WebElement]) -> list[str]:
    """
    One stable string per card for duplicate detection:

    - Prefer ``href:<viewjob-url-path>`` from the first ``a[href*="/viewjob/"]`` in the card.
    - If no href (or empty), use ``title:<normalized-first-line>`` using the same idea as
      ``_card_label`` (img alt, then job link text line, then first line of card text).
    - Empty string means "unknown" — caller should **keep** the card (do not collapse with others).
    """
    if not cards:
        return []
    chunk_size = max(10, int(os.getenv("HIRECAFE_CARD_DEDUPE_CHUNK", "80")))
    keys: list[str] = []
    for start in range(0, len(cards), chunk_size):
        chunk = cards[start : start + chunk_size]
        try:
            part = driver.execute_script(
                """
                const norm = (s) => String(s || '').trim().toLowerCase().replace(/\\s+/g, ' ');
                const nodes = Array.from(arguments);
                return nodes.map((card) => {
                  const a = card.querySelector('a[href*="/viewjob/"]');
                  if (a) {
                    let h = (a.getAttribute('href') || '').trim();
                    if (h) {
                      h = h.split('?')[0].split('#')[0];
                      return 'href:' + h;
                    }
                  }
                  const img = card.querySelector('img[alt]');
                  if (img && img.alt && String(img.alt).trim())
                    return 'title:' + norm(String(img.alt).trim()).slice(0, 200);
                  if (a) {
                    let el = a;
                    for (let k = 0; k < 10 && el; k++) {
                      const t = (el.innerText || '').trim();
                      if (t && t.length > 2 && t.length < 180) {
                        const line = t.split(/\\r?\\n/)[0].trim();
                        if (line) return 'title:' + norm(line).slice(0, 200);
                      }
                      el = el.parentElement;
                    }
                  }
                  const raw = (card.innerText || '').trim();
                  const first = raw.split(/\\r?\\n/).map(x => x.trim()).filter(Boolean)[0];
                  if (first) return 'title:' + norm(first).slice(0, 200);
                  return '';
                });
                """,
                *chunk,
            )
            keys.extend(str(x) for x in (part or []))
        except Exception:
            keys.extend([""] * len(chunk))
    while len(keys) < len(cards):
        keys.append("")
    return keys[: len(cards)]


def _dedupe_card_elements(driver: Any, cards: list[WebElement]) -> list[WebElement]:
    """
    1. **Structural:** drop card nodes that are strict descendants of another node in the list
       (nested wrappers from the same grid cell).

    2. **Visible job:** drop later cards that duplicate an earlier card’s primary ``/viewjob/``
       href, or — when no href is present — the same normalized visible job title (first line),
       mirroring ``_card_label`` heuristics.

    Step 1 uses one browser round-trip. Step 2 batches key extraction (chunked) then dedupes in Python.
    """
    if len(cards) <= 1:
        return cards

    indices = _structural_dedupe_card_indices(driver, cards)
    if indices is None:
        logger.warning(
            "hirecafe carousel: structural dedupe failed; continuing without structural dedupe",
        )
        structural = cards
    else:
        structural = [cards[i] for i in indices]

    if len(structural) <= 1:
        return structural

    keys = _visible_job_dedupe_keys_for_cards(driver, structural)
    seen: set[str] = set()
    out: list[WebElement] = []
    dropped_title = 0
    for i, card in enumerate(structural):
        k = keys[i] if i < len(keys) else ""
        if not k:
            out.append(card)
            continue
        if k in seen:
            dropped_title += 1
            continue
        seen.add(k)
        out.append(card)

    if dropped_title:
        logger.info(
            "hirecafe carousel: dropped %s duplicate card(s) by href/title key (after structural dedupe)",
            dropped_title,
        )
    return out


def _filter_cards_in_viewport(driver: Any, cards: list[WebElement]) -> list[WebElement]:
    """
    Keep only elements whose bounding box intersects the browser viewport
    (any overlap with ``0..innerWidth`` x ``0..innerHeight``).
    Chunked to avoid WebDriver argument limits on large grids.
    """
    if not cards:
        return []
    chunk_size = max(10, int(os.getenv("HIRECAFE_VIEWPORT_FILTER_CHUNK", "80")))
    kept: list[WebElement] = []
    for start in range(0, len(cards), chunk_size):
        chunk = cards[start : start + chunk_size]
        try:
            indices = driver.execute_script(
                """
                const nodes = arguments;
                const vw = window.innerWidth;
                const vh = window.innerHeight;
                const out = [];
                for (let i = 0; i < nodes.length; i++) {
                  const r = nodes[i].getBoundingClientRect();
                  const vis = r.width > 0 && r.height > 0
                    && r.bottom > 0 && r.right > 0
                    && r.top < vh && r.left < vw;
                  if (vis) out.push(i);
                }
                return out;
                """,
                *chunk,
            )
            for i in indices or []:
                kept.append(chunk[int(i)])
        except Exception:
            for c in chunk:
                try:
                    ok = driver.execute_script(
                        """
                        const r = arguments[0].getBoundingClientRect();
                        const vw = window.innerWidth;
                        const vh = window.innerHeight;
                        return r.width > 0 && r.height > 0 && r.bottom > 0 && r.right > 0
                          && r.top < vh && r.left < vw;
                        """,
                        c,
                    )
                    if ok:
                        kept.append(c)
                except Exception:
                    continue
    return kept


def list_viewport_company_cards(driver: Any) -> list[WebElement]:
    """
    Company card roots in the infinite-scroll feed that are **in the current viewport**
    and look like a job card (``/viewjob/`` link and/or carousel buttons).

    Used by the HireCafe scraper to expand carousels only for on-screen cards after
    each scroll tick (avoids walking hundreds of off-screen nodes every time).
    """
    root = _infinite_scroll_root(driver)
    if not root:
        return []

    cards = _grid_children_cards(root)
    cards = _dedupe_card_elements(driver, cards)
    candidates: list[WebElement] = []
    for card in cards:
        try:
            if not card.is_displayed():
                continue
        except Exception:
            continue
        try:
            has_job = card.find_elements(By.CSS_SELECTOR, 'a[href*="/viewjob/"]')
            has_btns = card.find_elements(By.TAG_NAME, "button")
            if not has_job and not has_btns:
                continue
        except Exception:
            continue
        candidates.append(card)

    return _filter_cards_in_viewport(driver, candidates)


def _card_label(driver: Any, card: WebElement) -> str:
    """Short human-readable label for logging (company name or first line of card text)."""
    try:
        s = driver.execute_script(
            """
            const card = arguments[0];
            const img = card.querySelector('img[alt]');
            if (img && img.alt && String(img.alt).trim())
              return String(img.alt).trim().slice(0, 200);
            const job = card.querySelector('a[href*="/viewjob/"]');
            if (job) {
              let el = job;
              for (let i = 0; i < 10 && el; i++) {
                const t = (el.innerText || '').trim();
                if (t && t.length > 2 && t.length < 180) {
                  const line = t.split(/\\r?\\n/)[0].trim();
                  if (line) return line.slice(0, 200);
                }
                el = el.parentElement;
              }
            }
            const raw = (card.innerText || '').trim();
            const first = raw.split(/\\r?\\n/).map(x => x.trim()).filter(Boolean)[0];
            return (first || 'Unknown card').slice(0, 200);
            """,
            card,
        )
        return str(s or "Unknown card").strip() or "Unknown card"
    except Exception:
        return "Unknown card"


def expand_all_visible_company_cards_detailed(driver: Any) -> list[CardJobsRow]:
    """
    Like ``expand_all_visible_company_cards`` but returns one row per processed card:
    ``card_index``, ``grid_index``, ``label``, ``job_count``, ``viewjob_hrefs`` (sorted list).
    """
    root = _infinite_scroll_root(driver)
    if not root:
        logger.warning("hirecafe carousel: infinite-scroll-component not found")
        return []

    cards = _grid_children_cards(root)
    logger.info(
        "hirecafe carousel: found %s raw grid cell(s); deduping nested wrappers (fast batch)...",
        len(cards),
    )
    cards = _dedupe_card_elements(driver, cards)
    if not cards:
        logger.warning("hirecafe carousel: no grid cards under infinite-scroll-component")
        return []

    logger.info(
        "hirecafe carousel: starting — %s company card(s); clicking each card’s carousel next "
        "(this is the slow step; one line per card when each finishes)",
        len(cards),
    )
    rows: list[CardJobsRow] = []
    card_seq = 0
    for idx, card in enumerate(cards):
        try:
            if not card.is_displayed():
                continue
        except Exception:
            continue
        try:
            has_job = card.find_elements(By.CSS_SELECTOR, 'a[href*="/viewjob/"]')
            has_btns = card.find_elements(By.TAG_NAME, "button")
            if not has_job and not has_btns:
                continue
        except Exception:
            continue

        label = _card_label(driver, card)
        lab = label if len(label) <= 70 else label[:67] + "..."
        logger.info(
            "hirecafe carousel: expanding %s/%s (grid slot %s) %r — clicking carousel next…",
            card_seq + 1,
            len(cards),
            idx + 1,
            lab,
        )
        part = expand_carousels_in_card(driver, card)
        hrefs_sorted = sorted(part)
        rows.append(
            {
                "card_index": card_seq,
                "grid_index": idx,
                "label": label,
                "job_count": len(part),
                "viewjob_hrefs": hrefs_sorted,
            }
        )
        logger.info(
            "hirecafe carousel: done %s/%s — %s job URL(s) on this card",
            card_seq + 1,
            len(cards),
            len(part),
        )
        card_seq += 1

    union = {u for r in rows for u in r.get("viewjob_hrefs", [])}
    logger.info(
        "hirecafe carousel: expanded %s company cards, total unique viewjob hrefs=%s",
        len(rows),
        len(union),
    )
    return rows


def expand_all_visible_company_cards(driver: Any) -> set[str]:
    """
    Find all job cards in ``div.infinite-scroll-component`` grid, expand each carousel,
    return union of all ``/viewjob/`` URLs discovered.
    """
    rows = expand_all_visible_company_cards_detailed(driver)
    return {u for r in rows for u in r.get("viewjob_hrefs", [])}
