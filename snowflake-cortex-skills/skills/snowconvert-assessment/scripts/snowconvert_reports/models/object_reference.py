from dataclasses import dataclass


@dataclass(frozen=True)
class ObjectReference:
    """A single row from ObjectReferences.*.csv."""

    caller_full_name: str
    caller_code_unit: str
    referenced_full_name: str
    referenced_element_type: str
    relation_type: str
    line: str
    file_name: str

    @property
    def is_missing_reference(self) -> bool:
        return self.referenced_element_type == "MISSING"

    @staticmethod
    def from_csv_row(row: dict[str, str]) -> "ObjectReference":
        return ObjectReference(
            caller_full_name=row.get("Caller_CodeUnit_FullName", "").strip(),
            caller_code_unit=row.get("Caller_CodeUnit", "").strip(),
            referenced_full_name=row.get("Referenced_Element_FullName", "").strip(),
            referenced_element_type=row.get("Referenced_Element_Type", "").strip(),
            relation_type=row.get("Relation_Type", "").strip(),
            line=row.get("Line", "").strip(),
            file_name=row.get("FileName", "").strip(),
        )
