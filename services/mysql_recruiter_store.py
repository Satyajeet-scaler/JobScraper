import json
import os
from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import urlparse, urlunparse

import pymysql


def _mysql_conn_kwargs() -> dict[str, Any]:
    host = (os.getenv("MYSQLHOST") or "").strip()
    port = int((os.getenv("MYSQLPORT") or "3306").strip() or "3306")
    user = (os.getenv("MYSQLUSER") or "").strip()
    password = (os.getenv("MYSQLPASSWORD") or "").strip()
    database = (os.getenv("MYSQLDATABASE") or "").strip()
    if not all((host, user, password, database)):
        raise RuntimeError(
            "Missing MySQL connection env vars. Required: MYSQLHOST, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE."
        )
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }


@contextmanager
def _db() -> Iterator[pymysql.connections.Connection]:
    conn = pymysql.connect(**_mysql_conn_kwargs())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _json_dump(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalized_linkedin_profile_url(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.rstrip("/")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "", ""))


def upsert_lusha_recruiter(
    *,
    contact_id: str,
    request_id: str,
    search_contact: dict[str, Any],
    search_response: dict[str, Any],
    enrich_response: dict[str, Any],
    enriched_contact: dict[str, Any],
    source_app: str = "recruiter_profile_backfill",
) -> bool:
    if not contact_id.strip():
        return False

    data = enriched_contact.get("data")
    data_obj = data if isinstance(data, dict) else {}
    location = data_obj.get("location")
    location_obj = location if isinstance(location, dict) else {}
    social = data_obj.get("socialLinks")
    social_obj = social if isinstance(social, dict) else {}
    company = data_obj.get("company")
    company_obj = company if isinstance(company, dict) else {}

    linkedin_url = _normalized_linkedin_profile_url(
        social_obj.get("linkedin") or enriched_contact.get("linkedinUrl")
    )

    sql = """
    INSERT INTO lusha_recruiters (
        lusha_contact_id, lusha_request_id, full_name, first_name, last_name, job_title,
        company_id, company_name, company_fqdn, linkedin_url, twitter_url,
        city, state, country, country_iso2, continent, is_shown, is_enrich_success,
        linkedin_followers_count, linkedin_connections_count,
        has_emails, has_work_email, has_private_email, has_phones, has_mobile_phone, has_direct_phone,
        has_company_employees_count, has_company_revenue, has_company_main_industry, has_company_sub_industry,
        has_company_funding, has_company_intent, has_company_technologies, has_department, has_seniority,
        has_contact_location, has_social_link, main_industry, sub_industry,
        signal_types_json, departments_json, seniority_json, email_addresses_json, phone_numbers_json,
        linkedin_certifications_json, linkedin_courses_json, linkedin_awards_json, linkedin_skills_json,
        company_funding_json, company_intent_json, company_technologies_json, company_revenue_range_json,
        raw_lusha_search_json, raw_lusha_enrich_json, source_app
    ) VALUES (
        %(lusha_contact_id)s, %(lusha_request_id)s, %(full_name)s, %(first_name)s, %(last_name)s, %(job_title)s,
        %(company_id)s, %(company_name)s, %(company_fqdn)s, %(linkedin_url)s, %(twitter_url)s,
        %(city)s, %(state)s, %(country)s, %(country_iso2)s, %(continent)s, %(is_shown)s, %(is_enrich_success)s,
        %(linkedin_followers_count)s, %(linkedin_connections_count)s,
        %(has_emails)s, %(has_work_email)s, %(has_private_email)s, %(has_phones)s, %(has_mobile_phone)s, %(has_direct_phone)s,
        %(has_company_employees_count)s, %(has_company_revenue)s, %(has_company_main_industry)s, %(has_company_sub_industry)s,
        %(has_company_funding)s, %(has_company_intent)s, %(has_company_technologies)s, %(has_department)s, %(has_seniority)s,
        %(has_contact_location)s, %(has_social_link)s, %(main_industry)s, %(sub_industry)s,
        CAST(%(signal_types_json)s AS JSON), CAST(%(departments_json)s AS JSON), CAST(%(seniority_json)s AS JSON), CAST(%(email_addresses_json)s AS JSON), CAST(%(phone_numbers_json)s AS JSON),
        CAST(%(linkedin_certifications_json)s AS JSON), CAST(%(linkedin_courses_json)s AS JSON), CAST(%(linkedin_awards_json)s AS JSON), CAST(%(linkedin_skills_json)s AS JSON),
        CAST(%(company_funding_json)s AS JSON), CAST(%(company_intent_json)s AS JSON), CAST(%(company_technologies_json)s AS JSON), CAST(%(company_revenue_range_json)s AS JSON),
        CAST(%(raw_lusha_search_json)s AS JSON), CAST(%(raw_lusha_enrich_json)s AS JSON), %(source_app)s
    )
    ON DUPLICATE KEY UPDATE
        lusha_request_id = VALUES(lusha_request_id),
        full_name = VALUES(full_name),
        first_name = VALUES(first_name),
        last_name = VALUES(last_name),
        job_title = VALUES(job_title),
        company_id = VALUES(company_id),
        company_name = VALUES(company_name),
        company_fqdn = VALUES(company_fqdn),
        linkedin_url = VALUES(linkedin_url),
        twitter_url = VALUES(twitter_url),
        city = VALUES(city),
        state = VALUES(state),
        country = VALUES(country),
        country_iso2 = VALUES(country_iso2),
        continent = VALUES(continent),
        is_shown = VALUES(is_shown),
        is_enrich_success = VALUES(is_enrich_success),
        linkedin_followers_count = VALUES(linkedin_followers_count),
        linkedin_connections_count = VALUES(linkedin_connections_count),
        has_emails = VALUES(has_emails),
        has_work_email = VALUES(has_work_email),
        has_private_email = VALUES(has_private_email),
        has_phones = VALUES(has_phones),
        has_mobile_phone = VALUES(has_mobile_phone),
        has_direct_phone = VALUES(has_direct_phone),
        has_company_employees_count = VALUES(has_company_employees_count),
        has_company_revenue = VALUES(has_company_revenue),
        has_company_main_industry = VALUES(has_company_main_industry),
        has_company_sub_industry = VALUES(has_company_sub_industry),
        has_company_funding = VALUES(has_company_funding),
        has_company_intent = VALUES(has_company_intent),
        has_company_technologies = VALUES(has_company_technologies),
        has_department = VALUES(has_department),
        has_seniority = VALUES(has_seniority),
        has_contact_location = VALUES(has_contact_location),
        has_social_link = VALUES(has_social_link),
        main_industry = VALUES(main_industry),
        sub_industry = VALUES(sub_industry),
        signal_types_json = VALUES(signal_types_json),
        departments_json = VALUES(departments_json),
        seniority_json = VALUES(seniority_json),
        email_addresses_json = VALUES(email_addresses_json),
        phone_numbers_json = VALUES(phone_numbers_json),
        linkedin_certifications_json = VALUES(linkedin_certifications_json),
        linkedin_courses_json = VALUES(linkedin_courses_json),
        linkedin_awards_json = VALUES(linkedin_awards_json),
        linkedin_skills_json = VALUES(linkedin_skills_json),
        company_funding_json = VALUES(company_funding_json),
        company_intent_json = VALUES(company_intent_json),
        company_technologies_json = VALUES(company_technologies_json),
        company_revenue_range_json = VALUES(company_revenue_range_json),
        raw_lusha_search_json = VALUES(raw_lusha_search_json),
        raw_lusha_enrich_json = VALUES(raw_lusha_enrich_json),
        source_app = VALUES(source_app),
        updated_at = CURRENT_TIMESTAMP
    """

    payload = {
        "lusha_contact_id": contact_id.strip(),
        "lusha_request_id": request_id.strip() or None,
        "full_name": data_obj.get("fullName") or search_contact.get("name"),
        "first_name": data_obj.get("firstName"),
        "last_name": data_obj.get("lastName"),
        "job_title": data_obj.get("jobTitle") or search_contact.get("jobTitle"),
        "company_id": _safe_int(data_obj.get("companyId") or search_contact.get("companyId")),
        "company_name": data_obj.get("companyName") or search_contact.get("companyName"),
        "company_fqdn": search_contact.get("fqdn"),
        "linkedin_url": linkedin_url,
        "twitter_url": social_obj.get("xUrl"),
        "city": location_obj.get("city"),
        "state": location_obj.get("state"),
        "country": location_obj.get("country"),
        "country_iso2": location_obj.get("country_iso2"),
        "continent": location_obj.get("continent"),
        "is_shown": data_obj.get("isShown"),
        "is_enrich_success": enriched_contact.get("isSuccess"),
        "linkedin_followers_count": _safe_int(data_obj.get("linkedinFollowersCount")),
        "linkedin_connections_count": _safe_int(data_obj.get("linkedinConnectionsCount")),
        "has_emails": search_contact.get("hasEmails"),
        "has_work_email": search_contact.get("hasWorkEmail"),
        "has_private_email": search_contact.get("hasPrivateEmail"),
        "has_phones": search_contact.get("hasPhones"),
        "has_mobile_phone": search_contact.get("hasMobilePhone"),
        "has_direct_phone": search_contact.get("hasDirectPhone"),
        "has_company_employees_count": search_contact.get("hasCompanyEmployeesCount"),
        "has_company_revenue": search_contact.get("hasCompanyRevenue"),
        "has_company_main_industry": search_contact.get("hasCompanyMainIndustry"),
        "has_company_sub_industry": search_contact.get("hasCompanySubIndustry"),
        "has_company_funding": search_contact.get("hasCompanyFunding"),
        "has_company_intent": search_contact.get("hasCompanyIntent"),
        "has_company_technologies": search_contact.get("hasCompanyTechnologies"),
        "has_department": search_contact.get("hasDepartment"),
        "has_seniority": search_contact.get("hasSeniority"),
        "has_contact_location": search_contact.get("hasContactLocation"),
        "has_social_link": search_contact.get("hasSocialLink"),
        "main_industry": company_obj.get("mainIndustry"),
        "sub_industry": company_obj.get("subIndustry"),
        "signal_types_json": _json_dump(search_contact.get("signalTypes") or []),
        "departments_json": _json_dump(data_obj.get("departments") or []),
        "seniority_json": _json_dump(data_obj.get("seniority") or []),
        "email_addresses_json": _json_dump(data_obj.get("emailAddresses") or []),
        "phone_numbers_json": _json_dump(data_obj.get("phoneNumbers") or []),
        "linkedin_certifications_json": _json_dump(data_obj.get("linkedinCertifications") or []),
        "linkedin_courses_json": _json_dump(data_obj.get("linkedinCourses") or []),
        "linkedin_awards_json": _json_dump(data_obj.get("linkedinAwards") or []),
        "linkedin_skills_json": _json_dump(data_obj.get("linkedinSkills") or []),
        "company_funding_json": _json_dump(company_obj.get("funding") or {}),
        "company_intent_json": _json_dump(company_obj.get("intent") or {}),
        "company_technologies_json": _json_dump(company_obj.get("technologies") or []),
        "company_revenue_range_json": _json_dump(company_obj.get("revenueRange") or []),
        "raw_lusha_search_json": _json_dump(search_response),
        "raw_lusha_enrich_json": _json_dump(enrich_response),
        "source_app": source_app,
    }

    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)
    return True
