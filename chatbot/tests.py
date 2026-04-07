from unittest.mock import patch

from django.test import SimpleTestCase

from .services.ollama_service import extract_problem_analytics_query
from .services.sqlserver_job_card_analytics_service import (
    _build_query_terms,
    classify_frequency,
    classify_trend,
)
from .services.system_health_service import (
    STATUS_ERROR,
    STATUS_OK,
    STATUS_WARNING,
    get_system_health_report,
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


class SystemHealthReportTests(SimpleTestCase):
    @patch("chatbot.services.system_health_service.check_sync_checkpoint_health")
    @patch("chatbot.services.system_health_service.check_sqlserver_health")
    @patch("chatbot.services.system_health_service.check_ollama_health")
    def test_get_system_health_report_aggregates_warning(
        self,
        mock_ollama,
        mock_sqlserver,
        mock_checkpoint,
    ):
        mock_ollama.return_value = {
            "name": "ollama",
            "label": "Ollama",
            "status": STATUS_OK,
            "status_label": "ปกติ",
            "message": "ok",
            "details": {},
            "alerts": [],
        }
        mock_sqlserver.return_value = {
            "name": "sqlserver",
            "label": "SQL Server",
            "status": STATUS_WARNING,
            "status_label": "เตือน",
            "message": "warning",
            "details": {},
            "alerts": ["sql warning"],
        }
        mock_checkpoint.return_value = {
            "name": "sync_checkpoints",
            "label": "Sync Checkpoint",
            "status": STATUS_OK,
            "status_label": "ปกติ",
            "message": "ok",
            "details": {},
            "alerts": [],
        }

        report = get_system_health_report(include_live_checks=True)

        self.assertEqual(report["status"], STATUS_WARNING)
        self.assertIn("sql warning", report["alerts"])

    @patch("chatbot.services.system_health_service.check_sync_checkpoint_health")
    @patch("chatbot.services.system_health_service.check_sqlserver_health")
    @patch("chatbot.services.system_health_service.check_ollama_health")
    def test_get_system_health_report_aggregates_error(
        self,
        mock_ollama,
        mock_sqlserver,
        mock_checkpoint,
    ):
        mock_ollama.return_value = {
            "name": "ollama",
            "label": "Ollama",
            "status": STATUS_ERROR,
            "status_label": "ผิดปกติ",
            "message": "down",
            "details": {},
            "alerts": ["ollama down"],
        }
        mock_sqlserver.return_value = {
            "name": "sqlserver",
            "label": "SQL Server",
            "status": STATUS_OK,
            "status_label": "ปกติ",
            "message": "ok",
            "details": {},
            "alerts": [],
        }
        mock_checkpoint.return_value = {
            "name": "sync_checkpoints",
            "label": "Sync Checkpoint",
            "status": STATUS_OK,
            "status_label": "ปกติ",
            "message": "ok",
            "details": {},
            "alerts": [],
        }

        report = get_system_health_report(include_live_checks=True)

        self.assertEqual(report["status"], STATUS_ERROR)
        self.assertIn("ollama down", report["alerts"])
