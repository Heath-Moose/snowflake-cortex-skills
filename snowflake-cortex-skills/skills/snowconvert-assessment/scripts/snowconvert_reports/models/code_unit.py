from dataclasses import dataclass


def _safe_int(val: str) -> int:
    val = val.strip() if val else ""
    try:
        return int(val) if val else 0
    except ValueError:
        return 0


@dataclass(frozen=True)
class TopLevelCodeUnit:
    """A single row from TopLevelCodeUnits.csv."""

    code_unit_id: str
    code_unit_name: str
    category: str
    file_name: str
    code_unit: str
    deployment_order: str
    has_missing_dependencies: bool
    conversion_status: str
    lines_of_code: int
    ewi_count: int
    fdm_count: int
    prf_count: int
    highest_ewi_severity: str
    line_number: int = 0

    @staticmethod
    def from_csv_row(row: dict[str, str]) -> "TopLevelCodeUnit":
        deployment_raw = row.get("Deployment Order", "").strip()
        has_missing = "*" in deployment_raw
        clean_order = deployment_raw.replace("*", "")

        return TopLevelCodeUnit(
            code_unit_id=row.get("CodeUnitId", "").strip(),
            code_unit_name=row.get("CodeUnitName", "").strip(),
            category=row.get("Category", "").strip(),
            file_name=row.get("FileName", "").strip(),
            code_unit=row.get("CodeUnit", "").strip(),
            deployment_order=clean_order,
            has_missing_dependencies=has_missing,
            conversion_status=row.get(
                "ConversionStatus", row.get("Conversion", "")
            ).strip(),
            lines_of_code=_safe_int(
                row.get("Lines of Code", row.get("LinesOfCode", "0"))
            ),
            ewi_count=_safe_int(row.get("EWI Count", "0")),
            fdm_count=_safe_int(row.get("FDM Count", "0")),
            prf_count=_safe_int(row.get("PRF Count", "0")),
            highest_ewi_severity=row.get("HighestEWISeverity", "").strip(),
            line_number=_safe_int(row.get("LineNumber", "0")),
        )
