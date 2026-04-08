from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, List


NORMALIZE_PATTERN = re.compile(r"[^0-9a-z\u0E00-\u0E7F]+")
TOKEN_SPLIT_PATTERN = re.compile(r"[\s,/|]+")


@dataclass(frozen=True, slots=True)
class TermGroup:
    canonical: str
    aliases: tuple[str, ...]


TERM_GROUPS: tuple[TermGroup, ...] = (
    TermGroup(
        canonical="Sensor",
        aliases=(
            "sensor",
            "ps sensor",
            "proximity sensor",
            "prox sensor",
            "photo sensor",
            "photoelectric sensor",
            "เซ็นเซอร์",
        ),
    ),
    TermGroup(
        canonical="Solenoid Valve",
        aliases=(
            "solenoid valve",
            "solenoid",
            "coil solenoid valve",
        ),
    ),
    TermGroup(
        canonical="Emergency Stop",
        aliases=(
            "emergency stop",
            "emergency switch",
            "e stop",
            "e-stop",
            "ปุ่ม emergency",
        ),
    ),
    TermGroup(
        canonical="Conveyor Magnet",
        aliases=(
            "conveyor mag net",
            "conveyor magnet",
            "magnet conveyor",
            "mag net",
        ),
    ),
    TermGroup(
        canonical="Relay",
        aliases=(
            "relay",
            "ly4n",
        ),
    ),
)


def normalize_grouping_text(text: str) -> str:
    normalized = NORMALIZE_PATTERN.sub(" ", (text or "").strip().lower())
    return " ".join(normalized.split())


def _contains_phrase(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    pattern = rf"(?<!\S){re.escape(phrase)}(?!\S)"
    return re.search(pattern, text) is not None


def _remove_phrase(text: str, phrase: str) -> str:
    if not text or not phrase:
        return text
    pattern = rf"(?<!\S){re.escape(phrase)}(?!\S)"
    return re.sub(pattern, " ", text)


def _deduplicate_terms(terms: Iterable[str]) -> list[str]:
    unique_terms: list[str] = []
    seen = set()

    for term in terms:
        cleaned = " ".join((term or "").split()).strip()
        if not cleaned:
            continue
        normalized = normalize_grouping_text(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_terms.append(cleaned)

    return unique_terms


def find_matching_term_groups(texts: Iterable[str]) -> list[TermGroup]:
    normalized_text = " ".join(
        normalize_grouping_text(text)
        for text in texts
        if normalize_grouping_text(text)
    ).strip()
    if not normalized_text:
        return []

    matches: list[TermGroup] = []
    for group in TERM_GROUPS:
        normalized_terms = _deduplicate_terms((group.canonical, *group.aliases))
        if any(_contains_phrase(normalized_text, normalize_grouping_text(term)) for term in normalized_terms):
            matches.append(group)

    return matches


def build_semantic_search_groups(query: str) -> list[list[str]]:
    normalized_query = normalize_grouping_text(query)
    if not normalized_query:
        return []

    remaining_text = normalized_query
    groups: list[list[str]] = []

    for group in TERM_GROUPS:
        group_terms = _deduplicate_terms((group.canonical, *group.aliases))
        normalized_terms = sorted(
            (normalize_grouping_text(term) for term in group_terms),
            key=len,
            reverse=True,
        )
        if any(_contains_phrase(normalized_query, term) for term in normalized_terms):
            groups.append(group_terms)
            for normalized_term in normalized_terms:
                remaining_text = _remove_phrase(remaining_text, normalized_term)

    remaining_terms: list[list[str]] = []
    for raw_term in TOKEN_SPLIT_PATTERN.split(remaining_text):
        cleaned = raw_term.strip()
        if not cleaned:
            continue
        if len(cleaned) == 1 and cleaned.isascii():
            continue
        remaining_terms.append([cleaned])

    return groups + remaining_terms


def build_semantic_search_text(query: str) -> str:
    normalized_query = " ".join((query or "").split()).strip()
    if not normalized_query:
        return ""

    groups = build_semantic_search_groups(normalized_query)
    expanded_terms = _deduplicate_terms(
        term
        for group in groups
        for term in group
    )

    if not expanded_terms:
        return normalized_query

    raw_terms = _deduplicate_terms(TOKEN_SPLIT_PATTERN.split(normalize_grouping_text(query)))
    if normalize_grouping_text(" ".join(expanded_terms)) == normalize_grouping_text(" ".join(raw_terms)):
        return normalized_query

    return (
        f"{normalized_query}\n"
        f"คำค้นใกล้เคียง: {', '.join(expanded_terms[:12])}"
    )


def build_semantic_keyword_lines(*texts: str) -> list[str]:
    lines: list[str] = []
    for group in find_matching_term_groups(texts):
        keywords = _deduplicate_terms((group.canonical, *group.aliases))
        lines.append(f"คำค้นใกล้เคียงกลุ่ม {group.canonical}: {', '.join(keywords[:8])}")
    return lines
