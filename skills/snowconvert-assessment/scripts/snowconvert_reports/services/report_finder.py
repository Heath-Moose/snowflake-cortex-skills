from pathlib import Path
from typing import Optional


class ReportFinder:
    """Discover SnowConvert report files in a directory using glob patterns."""

    def __init__(self, reports_dir: str | Path):
        self.reports_dir = Path(reports_dir)

    def find(self, base_name: str, extension: str = "csv") -> Optional[Path]:
        """Find the most recent report file matching ``{base_name}.*.{extension}``."""
        matches = self.find_all(base_name, extension)
        return matches[0] if matches else None

    def find_all(self, base_name: str, extension: str = "csv") -> list[Path]:
        """Find all matching report files, newest first."""
        pattern = f"{base_name}.*.{extension}"
        return sorted(
            self.reports_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

    def find_elements(self) -> Optional[Path]:
        return self.find("Elements")

    def find_issues(self) -> Optional[Path]:
        return self.find("Issues")

    def find_code_units(self) -> Optional[Path]:
        return self.find("TopLevelCodeUnits")

    def find_object_references(self) -> Optional[Path]:
        return self.find("ObjectReferences")

    def find_partition_membership(self) -> Optional[Path]:
        return self.find("PartitionMembership")

    def find_issues_estimation_json(self) -> Optional[Path]:
        return self.find("IssuesEstimation", extension="json")

    def find_toplevel_objects_estimation(self) -> Optional[Path]:
        return self.find("TopLevelObjectsEstimation")

    def find_issues_aggregate(self) -> Optional[Path]:
        return self.find("IssuesEstimationAggregate")

    def find_effort_formula(self) -> Optional[Path]:
        return self.find("EffortEstimationFormula")
