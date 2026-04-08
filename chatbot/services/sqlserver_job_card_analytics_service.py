from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from typing import Any

from .sqlserver_service import fetch_rows
from .term_grouping_service import build_semantic_search_groups

SEARCHABLE_FIELDS = (
    "MC_NO",
    "Description",
    "LOCATION",
    "ASSIGN_TEAM",
    "REPAIR_DETAIL",
    "REPAIR_PROBLEM_BY",
    "Position_name",
    "Problem",
    "Problem_Cause",
    "Problem_detail",
    "REPAIR_FNAME1",
    "REPAIR_FNAME2",
    "REPAIR_FNAME3",
)
DEFAULT_TOP_CASES = 5
DEFAULT_TOP_GROUPS = 5
DEFAULT_MONTHLY_LIMIT = 24
TOKEN_SPLIT_PATTERN = re.compile(r"[\s,/|]+")
PRIMARY_PROBLEM_FIELD_SQL = (
    "COALESCE("
    "NULLIF(LTRIM(RTRIM(CAST([Description] AS NVARCHAR(MAX)))), ''), "
    "NULLIF(LTRIM(RTRIM(CAST([Problem] AS NVARCHAR(MAX)))), ''), "
    "NULLIF(LTRIM(RTRIM(CAST([Problem_Cause] AS NVARCHAR(MAX)))), ''), "
    "NULLIF(LTRIM(RTRIM(CAST([Position_name] AS NVARCHAR(MAX)))), '')"
    ")"
)

LANGUAGE_TEXT = {
    "th": {
        "summary_title": "สรุปภาพรวม",
        "query": "คำค้น",
        "total_count": "พบทั้งหมด",
        "frequency": "ระดับความถี่",
        "trend": "แนวโน้มช่วงล่าสุด",
        "date_range": "ช่วงข้อมูลที่พบ",
        "yearly": "สถิติรายปี",
        "monthly": "สถิติรายเดือน",
        "expanded_terms": "รวมคำใกล้เคียงในการค้นหา",
        "top_problem_patterns": "อาการ/ปัญหาที่พบบ่อย",
        "top_machines": "เครื่องจักรที่พบมาก",
        "top_positions": "ตำแหน่งที่เสียที่พบบ่อย",
        "top_teams": "ทีมที่เข้าซ่อมบ่อย",
        "recent_cases": "ตัวอย่างเคสล่าสุด",
        "times": "ครั้ง",
        "not_found": "ไม่พบข้อมูลที่ตรงกับคำค้นนี้ใน v_MT_JOB_CARD",
        "often_very_high": "บ่อยมาก",
        "often_high": "ค่อนข้างบ่อย",
        "often_medium": "เกิดเป็นระยะ",
        "often_low": "ไม่บ่อย",
        "trend_up": "เพิ่มขึ้น",
        "trend_down": "ลดลง",
        "trend_flat": "ทรงตัว",
        "trend_new": "เริ่มพบมากขึ้นในช่วงล่าสุด",
        "trend_none": "ยังประเมินแนวโน้มไม่ได้",
    },
    "en": {
        "summary_title": "Overview",
        "query": "Query",
        "total_count": "Total occurrences",
        "frequency": "Frequency",
        "trend": "Recent trend",
        "date_range": "Observed range",
        "yearly": "Yearly counts",
        "monthly": "Monthly counts",
        "expanded_terms": "Expanded search terms",
        "top_problem_patterns": "Top problem patterns",
        "top_machines": "Top machines",
        "top_positions": "Top positions",
        "top_teams": "Top repair teams",
        "recent_cases": "Recent cases",
        "times": "times",
        "not_found": "No matching records were found in v_MT_JOB_CARD.",
        "often_very_high": "very frequent",
        "often_high": "quite frequent",
        "often_medium": "occurs periodically",
        "often_low": "not frequent",
        "trend_up": "increasing",
        "trend_down": "decreasing",
        "trend_flat": "stable",
        "trend_new": "newly rising in the recent period",
        "trend_none": "not enough data to determine the trend",
    },
    "ja": {
        "summary_title": "概要",
        "query": "検索語",
        "total_count": "発生回数",
        "frequency": "頻度",
        "trend": "直近の傾向",
        "date_range": "確認できた期間",
        "yearly": "年別件数",
        "monthly": "月別件数",
        "expanded_terms": "検索に含めた類義語",
        "top_problem_patterns": "よくある不具合パターン",
        "top_machines": "発生が多い設備",
        "top_positions": "発生が多い位置",
        "top_teams": "対応が多いチーム",
        "recent_cases": "最新事例",
        "times": "件",
        "not_found": "v_MT_JOB_CARD に一致するデータが見つかりませんでした。",
        "often_very_high": "非常に多い",
        "often_high": "やや多い",
        "often_medium": "定期的に発生",
        "often_low": "少ない",
        "trend_up": "増加傾向",
        "trend_down": "減少傾向",
        "trend_flat": "横ばい",
        "trend_new": "直近で新たに増加傾向",
        "trend_none": "傾向を判断するにはデータが不足しています",
    },
}


def _quote_identifier(identifier: str) -> str:
    normalized = (identifier or "").strip()
    if not normalized:
        raise ValueError("ชื่อ schema/view/field ว่างไม่ได้")
    return f"[{normalized.replace(']', ']]')}]"


def _normalize_text_value(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    if text in {"-", "[NULL]", "NULL", "None"}:
        return ""

    return " ".join(text.split())


def _format_datetime_value(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return _normalize_text_value(value)


def _parse_date_boundary(value: str | None, *, end_of_day: bool = False) -> datetime | None:
    normalized = _normalize_text_value(value)
    if not normalized:
        return None

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed_date = date.fromisoformat(normalized)
        boundary_time = time.max if end_of_day else time.min
        parsed = datetime.combine(parsed_date, boundary_time)
    return parsed


def _build_query_terms(query: str) -> list[str]:
    normalized_query = _normalize_text_value(query)
    if not normalized_query:
        return []

    raw_terms = [term.strip() for term in TOKEN_SPLIT_PATTERN.split(normalized_query)]
    deduped_terms: list[str] = []
    seen = set()

    for term in raw_terms:
        normalized_term = _normalize_text_value(term)
        if not normalized_term:
            continue
        if len(normalized_term) == 1 and normalized_term.isascii():
            continue
        lowered = normalized_term.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped_terms.append(normalized_term)

    if not deduped_terms:
        return [normalized_query]

    return deduped_terms


def _build_problem_match_where(
    *,
    query: str,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> tuple[str, list[Any], list[str], list[str]]:
    query_terms = _build_query_terms(query)
    if not query_terms:
        raise ValueError("query is required")

    search_term_groups = build_semantic_search_groups(query)
    if not search_term_groups:
        search_term_groups = [[term] for term in query_terms]

    where_clauses = ["[ID] IS NOT NULL"]
    params: list[Any] = []

    searchable_fields_sql = [
        f"COALESCE(CAST({_quote_identifier(field)} AS NVARCHAR(MAX)), '')"
        for field in SEARCHABLE_FIELDS
    ]

    expanded_terms: list[str] = []

    for term_group in search_term_groups:
        group_clauses: list[str] = []
        for term in term_group:
            cleaned_term = _normalize_text_value(term)
            if not cleaned_term:
                continue
            expanded_terms.append(cleaned_term)
            like_value = f"%{cleaned_term}%"
            group_clauses.extend(f"{field_sql} LIKE ?" for field_sql in searchable_fields_sql)
            params.extend([like_value] * len(searchable_fields_sql))

        if group_clauses:
            where_clauses.append("(" + " OR ".join(group_clauses) + ")")

    if date_from is not None:
        where_clauses.append("[J_CREATE_DATE] >= ?")
        params.append(date_from)

    if date_to is not None:
        where_clauses.append("[J_CREATE_DATE] <= ?")
        params.append(date_to)

    expanded_terms = list(dict.fromkeys(expanded_terms))
    return " AND ".join(where_clauses), params, query_terms, expanded_terms


def _fetch_count_row(
    *,
    schema: str,
    view_name: str,
    where_sql: str,
    params: list[Any],
) -> dict[str, Any]:
    full_view_name = f"{_quote_identifier(schema)}.{_quote_identifier(view_name)}"
    rows = fetch_rows(
        f"""
        SELECT
            COUNT(*) AS total_count,
            COUNT(DISTINCT CONVERT(char(7), [J_CREATE_DATE], 126)) AS active_months,
            SUM(CASE WHEN [J_CREATE_DATE] >= DATEADD(day, -30, GETDATE()) THEN 1 ELSE 0 END) AS last_30_days,
            SUM(CASE WHEN [J_CREATE_DATE] >= DATEADD(day, -90, GETDATE()) THEN 1 ELSE 0 END) AS last_90_days,
            SUM(CASE WHEN [J_CREATE_DATE] >= DATEADD(day, -180, GETDATE()) AND [J_CREATE_DATE] < DATEADD(day, -90, GETDATE()) THEN 1 ELSE 0 END) AS previous_90_days,
            MIN([J_CREATE_DATE]) AS first_occurrence,
            MAX([J_CREATE_DATE]) AS last_occurrence
        FROM {full_view_name}
        WHERE {where_sql}
        """,
        params,
    )
    return rows[0] if rows else {}


def _fetch_group_counts(
    *,
    schema: str,
    view_name: str,
    field_sql: str,
    value_alias: str,
    where_sql: str,
    params: list[Any],
    top_n: int,
) -> list[dict[str, Any]]:
    full_view_name = f"{_quote_identifier(schema)}.{_quote_identifier(view_name)}"
    rows = fetch_rows(
        f"""
        SELECT TOP {max(1, int(top_n))}
            {field_sql} AS {value_alias},
            COUNT(*) AS item_count
        FROM {full_view_name}
        WHERE {where_sql}
            AND {field_sql} IS NOT NULL
            AND LTRIM(RTRIM(CAST({field_sql} AS NVARCHAR(MAX)))) <> ''
        GROUP BY {field_sql}
        ORDER BY item_count DESC, {field_sql} ASC
        """,
        params,
    )
    return [
        {
            "value": _normalize_text_value(row.get(value_alias)),
            "count": int(row.get("item_count") or 0),
        }
        for row in rows
        if _normalize_text_value(row.get(value_alias))
    ]


def _fetch_recent_cases(
    *,
    schema: str,
    view_name: str,
    where_sql: str,
    params: list[Any],
    limit: int,
) -> list[dict[str, Any]]:
    full_view_name = f"{_quote_identifier(schema)}.{_quote_identifier(view_name)}"
    rows = fetch_rows(
        f"""
        SELECT TOP {max(1, int(limit))}
            [ID],
            [MC_NO],
            [Description],
            [J_CREATE_DATE],
            [ASSIGN_TEAM],
            [Position_name],
            [Problem],
            [Problem_Cause],
            [Problem_detail],
            [REPAIR_DETAIL],
            [REPAIR_FNAME1],
            [REPAIR_FNAME2],
            [REPAIR_FNAME3]
        FROM {full_view_name}
        WHERE {where_sql}
        ORDER BY [J_CREATE_DATE] DESC, [ID] DESC
        """,
        params,
    )
    cases: list[dict[str, Any]] = []
    for row in rows:
        workers = [
            _normalize_text_value(row.get("REPAIR_FNAME1")),
            _normalize_text_value(row.get("REPAIR_FNAME2")),
            _normalize_text_value(row.get("REPAIR_FNAME3")),
        ]
        workers = [worker for worker in workers if worker]
        cases.append(
            {
                "id": _normalize_text_value(row.get("ID")),
                "machine_no": _normalize_text_value(row.get("MC_NO")),
                "description": _normalize_text_value(row.get("Description")),
                "date": _format_datetime_value(row.get("J_CREATE_DATE")),
                "assign_team": _normalize_text_value(row.get("ASSIGN_TEAM")),
                "position_name": _normalize_text_value(row.get("Position_name")),
                "problem": _normalize_text_value(row.get("Problem")),
                "problem_cause": _normalize_text_value(row.get("Problem_Cause")),
                "problem_detail": _normalize_text_value(row.get("Problem_detail")),
                "repair_detail": _normalize_text_value(row.get("REPAIR_DETAIL")),
                "workers": workers,
            }
        )
    return cases


def classify_frequency(total_count: int, active_months: int, last_90_days: int) -> str:
    if total_count <= 0:
        return "often_low"

    average_per_active_month = (
        float(total_count) / active_months if active_months > 0 else float(total_count)
    )

    if average_per_active_month >= 8 or last_90_days >= 20:
        return "often_very_high"
    if average_per_active_month >= 4 or last_90_days >= 10:
        return "often_high"
    if average_per_active_month >= 2 or last_90_days >= 4:
        return "often_medium"
    return "often_low"


def classify_trend(last_90_days: int, previous_90_days: int) -> str:
    if last_90_days <= 0 and previous_90_days <= 0:
        return "trend_none"
    if previous_90_days <= 0 and last_90_days > 0:
        return "trend_new"

    change_ratio = (last_90_days - previous_90_days) / max(previous_90_days, 1)
    if change_ratio >= 0.25:
        return "trend_up"
    if change_ratio <= -0.25:
        return "trend_down"
    return "trend_flat"


def get_problem_analytics_text(language: str) -> dict[str, str]:
    return LANGUAGE_TEXT.get(language, LANGUAGE_TEXT["th"])


def analyze_mt_job_card_problem(
    *,
    query: str,
    schema: str,
    view_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
    top_cases: int = DEFAULT_TOP_CASES,
    top_groups: int = DEFAULT_TOP_GROUPS,
    monthly_limit: int = DEFAULT_MONTHLY_LIMIT,
) -> dict[str, Any]:
    parsed_date_from = _parse_date_boundary(date_from, end_of_day=False)
    parsed_date_to = _parse_date_boundary(date_to, end_of_day=True)

    where_sql, params, query_terms, expanded_query_terms = _build_problem_match_where(
        query=query,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
    )

    full_view_name = f"{_quote_identifier(schema)}.{_quote_identifier(view_name)}"
    count_row = _fetch_count_row(
        schema=schema,
        view_name=view_name,
        where_sql=where_sql,
        params=params,
    )

    total_count = int(count_row.get("total_count") or 0)
    active_months = int(count_row.get("active_months") or 0)
    last_30_days = int(count_row.get("last_30_days") or 0)
    last_90_days = int(count_row.get("last_90_days") or 0)
    previous_90_days = int(count_row.get("previous_90_days") or 0)
    first_occurrence = _format_datetime_value(count_row.get("first_occurrence"))
    last_occurrence = _format_datetime_value(count_row.get("last_occurrence"))

    yearly_counts = fetch_rows(
        f"""
        SELECT
            YEAR([J_CREATE_DATE]) AS item_year,
            COUNT(*) AS item_count
        FROM {full_view_name}
        WHERE {where_sql}
            AND [J_CREATE_DATE] IS NOT NULL
        GROUP BY YEAR([J_CREATE_DATE])
        ORDER BY item_year DESC
        """,
        params,
    )
    yearly_counts = [
        {
            "year": int(row.get("item_year")),
            "count": int(row.get("item_count") or 0),
        }
        for row in yearly_counts
        if row.get("item_year") is not None
    ]

    monthly_counts = fetch_rows(
        f"""
        SELECT
            CONVERT(char(7), [J_CREATE_DATE], 126) AS item_month,
            COUNT(*) AS item_count
        FROM {full_view_name}
        WHERE {where_sql}
            AND [J_CREATE_DATE] IS NOT NULL
        GROUP BY CONVERT(char(7), [J_CREATE_DATE], 126)
        ORDER BY item_month DESC
        """,
        params,
    )
    monthly_counts = [
        {
            "month": _normalize_text_value(row.get("item_month")),
            "count": int(row.get("item_count") or 0),
        }
        for row in monthly_counts
        if _normalize_text_value(row.get("item_month"))
    ][: max(1, int(monthly_limit))]

    top_machines = _fetch_group_counts(
        schema=schema,
        view_name=view_name,
        field_sql=_quote_identifier("MC_NO"),
        value_alias="machine_no",
        where_sql=where_sql,
        params=params,
        top_n=top_groups,
    )
    top_problem_patterns = _fetch_group_counts(
        schema=schema,
        view_name=view_name,
        field_sql=PRIMARY_PROBLEM_FIELD_SQL,
        value_alias="problem_pattern",
        where_sql=where_sql,
        params=params,
        top_n=top_groups,
    )
    top_positions = _fetch_group_counts(
        schema=schema,
        view_name=view_name,
        field_sql=_quote_identifier("Position_name"),
        value_alias="position_name",
        where_sql=where_sql,
        params=params,
        top_n=top_groups,
    )
    top_teams = _fetch_group_counts(
        schema=schema,
        view_name=view_name,
        field_sql=_quote_identifier("ASSIGN_TEAM"),
        value_alias="assign_team",
        where_sql=where_sql,
        params=params,
        top_n=top_groups,
    )
    recent_cases = _fetch_recent_cases(
        schema=schema,
        view_name=view_name,
        where_sql=where_sql,
        params=params,
        limit=top_cases,
    )

    average_per_active_month = (
        round(total_count / active_months, 2) if active_months > 0 else float(total_count)
    )
    frequency_key = classify_frequency(total_count, active_months, last_90_days)
    trend_key = classify_trend(last_90_days, previous_90_days)

    return {
        "query": _normalize_text_value(query),
        "query_terms": query_terms,
        "expanded_query_terms": expanded_query_terms,
        "schema": schema,
        "view_name": view_name,
        "date_from": _format_datetime_value(parsed_date_from),
        "date_to": _format_datetime_value(parsed_date_to),
        "total_count": total_count,
        "active_months": active_months,
        "average_per_active_month": average_per_active_month,
        "last_30_days": last_30_days,
        "last_90_days": last_90_days,
        "previous_90_days": previous_90_days,
        "first_occurrence": first_occurrence,
        "last_occurrence": last_occurrence,
        "frequency_key": frequency_key,
        "trend_key": trend_key,
        "yearly_counts": yearly_counts,
        "monthly_counts": monthly_counts,
        "top_problem_patterns": top_problem_patterns,
        "top_machines": top_machines,
        "top_positions": top_positions,
        "top_teams": top_teams,
        "recent_cases": recent_cases,
    }


def build_problem_analytics_summary(
    analytics: dict[str, Any],
    *,
    language: str = "th",
) -> str:
    text = get_problem_analytics_text(language)
    total_count = int(analytics.get("total_count") or 0)
    if total_count <= 0:
        return text["not_found"]

    lines = [
        f"{text['summary_title']}:",
        f"- {text['query']}: {analytics.get('query')}",
        f"- {text['total_count']}: {total_count} {text['times']}",
        f"- {text['frequency']}: {text[analytics.get('frequency_key') or 'often_low']}",
        f"- {text['trend']}: {text[analytics.get('trend_key') or 'trend_none']}",
    ]

    first_occurrence = analytics.get("first_occurrence")
    last_occurrence = analytics.get("last_occurrence")
    if first_occurrence or last_occurrence:
        lines.append(
            f"- {text['date_range']}: {first_occurrence or '-'} -> {last_occurrence or '-'}"
        )

    yearly_counts = analytics.get("yearly_counts") or []
    if yearly_counts:
        lines.append("")
        lines.append(f"{text['yearly']}:")
        for item in yearly_counts[:5]:
            lines.append(f"- {item['year']}: {item['count']} {text['times']}")

    monthly_counts = analytics.get("monthly_counts") or []
    if monthly_counts:
        lines.append("")
        lines.append(f"{text['monthly']}:")
        for item in monthly_counts[:6]:
            lines.append(f"- {item['month']}: {item['count']} {text['times']}")

    expanded_query_terms = analytics.get("expanded_query_terms") or []
    if expanded_query_terms:
        normalized_expanded = [_normalize_text_value(item) for item in expanded_query_terms]
        normalized_expanded = [item for item in normalized_expanded if item]
        if len(normalized_expanded) > 1:
            lines.append("")
            lines.append(f"{text['expanded_terms']}:")
            lines.append(f"- {', '.join(normalized_expanded[:12])}")

    top_problem_patterns = analytics.get("top_problem_patterns") or []
    if top_problem_patterns:
        lines.append("")
        lines.append(f"{text['top_problem_patterns']}:")
        for item in top_problem_patterns[:5]:
            lines.append(f"- {item['value']}: {item['count']} {text['times']}")

    top_machines = analytics.get("top_machines") or []
    if top_machines:
        lines.append("")
        lines.append(f"{text['top_machines']}:")
        for item in top_machines[:3]:
            lines.append(f"- {item['value']}: {item['count']} {text['times']}")

    top_positions = analytics.get("top_positions") or []
    if top_positions:
        lines.append("")
        lines.append(f"{text['top_positions']}:")
        for item in top_positions[:3]:
            lines.append(f"- {item['value']}: {item['count']} {text['times']}")

    top_teams = analytics.get("top_teams") or []
    if top_teams:
        lines.append("")
        lines.append(f"{text['top_teams']}:")
        for item in top_teams[:3]:
            lines.append(f"- {item['value']}: {item['count']} {text['times']}")

    recent_cases = analytics.get("recent_cases") or []
    if recent_cases:
        lines.append("")
        lines.append(f"{text['recent_cases']}:")
        for index, item in enumerate(recent_cases[:3], start=1):
            detail_parts = [
                item.get("date") or "-",
                item.get("machine_no") or "-",
                item.get("description") or item.get("problem") or "-",
            ]
            lines.append(f"- {index}. {' | '.join(detail_parts)}")

    return "\n".join(lines).strip()


def build_problem_analytics_source(
    *,
    query: str,
    schema: str,
    view_name: str,
) -> dict[str, Any]:
    return {
        "title": f"SQL Analytics | {query}",
        "source": f"sqlserver-analytics:{schema}.{view_name}",
        "chunk_index": None,
        "document_id": None,
        "distance": None,
    }
