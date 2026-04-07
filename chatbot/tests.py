from django.test import SimpleTestCase

from .services.ollama_service import extract_problem_analytics_query
from .services.sqlserver_job_card_analytics_service import (
    _build_query_terms,
    classify_frequency,
    classify_trend,
)


class ProblemAnalyticsHelpersTests(SimpleTestCase):
    def test_build_query_terms_splits_query(self):
        self.assertEqual(
            _build_query_terms("Sensor ชำรุด"),
            ["Sensor", "ชำรุด"],
        )

    def test_classify_frequency_high(self):
        self.assertEqual(
            classify_frequency(total_count=120, active_months=12, last_90_days=30),
            "often_very_high",
        )

    def test_classify_trend_up(self):
        self.assertEqual(
            classify_trend(last_90_days=12, previous_90_days=6),
            "trend_up",
        )

    def test_extract_problem_analytics_query_from_followup(self):
        history = [
            {"role": "user", "content": "Sensor ชำรุด แก้ยังไง"},
            {"role": "assistant", "content": "ลองเปลี่ยน sensor"},
        ]
        self.assertEqual(
            extract_problem_analytics_query(history, "ปัญหานี้เกิดกี่ครั้ง"),
            "Sensor ชำรุด",
        )

    def test_extract_problem_analytics_query_from_short_count_followup(self):
        history = [
            {"role": "user", "content": "วิธีแก้ปัญหา Sensor ชำรุด"},
            {"role": "assistant", "content": "ลองเปลี่ยน proximity sensor"},
        ]
        self.assertEqual(
            extract_problem_analytics_query(history, "เกิดขึ้นกี่ครั้งหรอครับ"),
            "Sensor ชำรุด",
        )

    def test_extract_problem_analytics_query_from_monthly_followup(self):
        history = [
            {"role": "user", "content": "วิธีแก้ปัญหา Sensor ชำรุด"},
            {"role": "assistant", "content": "ลองเปลี่ยน proximity sensor"},
        ]
        self.assertEqual(
            extract_problem_analytics_query(history, "ต่อเดือนเป็นยังไง"),
            "Sensor ชำรุด",
        )

    def test_extract_problem_analytics_query_from_count_phrase_followup(self):
        history = [
            {"role": "user", "content": "วิธีแก้ปัญหา Sensor ชำรุด"},
            {"role": "assistant", "content": "ลองเปลี่ยน proximity sensor"},
        ]
        self.assertEqual(
            extract_problem_analytics_query(history, "เป็นจำนวนครั้งเท่าไหร่หรอครับ"),
            "Sensor ชำรุด",
        )

    def test_extract_problem_analytics_query_from_top_problem_followup(self):
        history = [
            {"role": "user", "content": "วิธีแก้ปัญหา Sensor"},
            {"role": "assistant", "content": "ลองตรวจ sensor"},
        ]
        self.assertEqual(
            extract_problem_analytics_query(history, "ปัญหายอดฮิตคือ"),
            "Sensor",
        )
