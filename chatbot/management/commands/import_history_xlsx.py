from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from ...services.xlsx_history_ingestion_service import (
    HISTORY_WORKBOOK_SHEET_NAME,
    ingest_history_workbook,
)


class Command(BaseCommand):
    help = "นำเข้าไฟล์ XLSX แบบชีต History-2024 เข้า knowledge base โดยแยก 1 row = 1 document"

    def add_arguments(self, parser):
        parser.add_argument(
            "file_path",
            help="path ของไฟล์ .xlsx ที่ต้องการนำเข้า",
        )
        parser.add_argument(
            "--sheet",
            dest="sheet_name",
            default=None,
            help=(
                "ชื่อชีตที่ต้องการใช้ ถ้าไม่ระบุระบบจะหาให้อัตโนมัติ "
                f"(เช่น {HISTORY_WORKBOOK_SHEET_NAME}, History-2023, History-2022)"
            ),
        )
        parser.add_argument(
            "--name",
            dest="display_name",
            default=None,
            help="ชื่อไฟล์ที่ต้องการให้แสดงใน knowledge base แทนชื่อไฟล์จริง",
        )

    def handle(self, *args, **options):
        file_path = Path(options["file_path"]).expanduser()
        raw_sheet_name = options.get("sheet_name")
        sheet_name = raw_sheet_name.strip() if raw_sheet_name else None
        display_name = options.get("display_name")

        if not file_path.exists():
            raise CommandError(f"ไม่พบไฟล์: {file_path}")

        if file_path.suffix.lower() != ".xlsx":
            raise CommandError("คำสั่งนี้รองรับเฉพาะไฟล์ .xlsx เท่านั้น")

        try:
            result = ingest_history_workbook(
                file_path,
                display_name=display_name or file_path.name,
                sheet_name=sheet_name,
            )
        except Exception as exc:
            raise CommandError(f"import ไฟล์ xlsx ไม่สำเร็จ: {exc}") from exc

        self.stdout.write(
            self.style.SUCCESS(
                f"import เสร็จแล้วจากไฟล์ {file_path.name}"
            )
        )
        self.stdout.write(f"sheet: {result.get('sheet_name') or sheet_name or 'auto-detect'}")
        self.stdout.write(f"document_count: {result.get('document_count', 0)}")
        self.stdout.write(f"created: {result.get('created_count', 0)}")
        self.stdout.write(f"updated: {result.get('updated_count', 0)}")
        self.stdout.write(f"skipped: {result.get('skipped_count', 0)}")
