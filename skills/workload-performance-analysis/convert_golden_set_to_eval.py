#!/usr/bin/env python3
"""Convert golden set CSV rows to Harbor eval tasks (skip first 3 rows)."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

CSV_PATH = Path(
    "/Users/zhuli/Workspace/cortex/cortexagent/aieverywhere/data/golden_set/"
    "workload_performance_optimization/workload_performance_optimization_golden_set.csv"
)
EVAL_DIR = Path("/Users/zhuli/Workspace/cortex-code-skills/evals/workload-performance-optimization")
SKILL_PATH = "../../sql/workload-performance-analysis"
START_INDEX = 3  # skip first three rows
END_INDEX: int | None = None


def _get_question(turns_json: str) -> str:
    try:
        turns = json.loads(turns_json)
    except json.JSONDecodeError:
        return turns_json.strip()

    if not turns:
        return ""

    first = turns[0]
    content = first.get("content", [])
    if content and isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                return item.get("text", "").strip()

    return json.dumps(first)


def _get_reference(reference_json: str) -> str:
    try:
        ref = json.loads(reference_json)
    except json.JSONDecodeError:
        return reference_json.strip()

    if isinstance(ref, dict):
        content = ref.get("content", [])
        if content and isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    return item.get("text", "").strip()
    return reference_json.strip()


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    if len(slug) > 60:
        slug = slug[:60].rstrip("-")
    return slug or "task"


def _write_task(task_dir: Path, question: str, reference: str) -> None:
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "tests").mkdir(exist_ok=True)

    instruction = f"""Answer the following question using Snowflake account usage views.

Question:
{question}

Write your final answer to `/app/output.txt` exactly.
"""
    (task_dir / "instruction.md").write_text(instruction, encoding="utf-8")

    task_toml = f"""version = \"1.0\"

[metadata]
name = \"{task_dir.name}\"
description = \"Generated from golden set\"
tags = [\"workload-performance-optimization\"]

[verifier]
timeout_sec = 900.0

[agent]
timeout_sec = 900.0

[environment]
dockerfile = \"docker/Dockerfile\"
build_timeout_sec = 600.0
cpus = 1
memory_mb = 4096
storage_mb = 10240
"""
    (task_dir / "task.toml").write_text(task_toml, encoding="utf-8")

    test_sh = """#!/bin/bash

# CTRF produces a standard test report in JSON format which is useful for logging.
uvx \
  --with pytest==8.4.1 \
  --with pytest-json-ctrf==0.3.5 \
  pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA

if [ $? -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi
"""
    test_sh_path = task_dir / "tests" / "test.sh"
    test_sh_path.write_text(test_sh, encoding="utf-8")
    test_sh_path.chmod(0o755)

    reference_literal = json.dumps(reference)
    question_literal = json.dumps(question)
    test_py = f"""import sys
sys.path.append('/tests')

from lib import OUTPUT_FILE, run_llm_verifier

REFERENCE_OUTPUT = {reference_literal}
QUESTION = {question_literal}


def test_output_file_exists():
    assert OUTPUT_FILE.exists(), "Expected /app/output.txt to be created"


def test_output_judgement():
    run_llm_verifier(QUESTION, REFERENCE_OUTPUT)
"""
    (task_dir / "tests" / "test_outputs.py").write_text(test_py, encoding="utf-8")

    # Copy shared lib.py for pytest imports
    lib_src = EVAL_DIR / "lib.py"
    if lib_src.exists():
        (task_dir / "lib.py").write_text(lib_src.read_text(), encoding="utf-8")
        (task_dir / "tests" / "lib.py").write_text(lib_src.read_text(), encoding="utf-8")


def _existing_task_paths(config_path: Path) -> list[str]:
    if not config_path.exists():
        return []
    return [
        line.split(":", 1)[1].strip().strip('"')
        for line in config_path.read_text().splitlines()
        if line.strip().startswith("- path:")
    ]


def main() -> None:
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise SystemExit("No rows found in golden set CSV")

    selected = rows[START_INDEX:END_INDEX]
    task_names: list[str] = []

    for row in selected:
        question = _get_question(row.get("TURNS", ""))
        reference = _get_reference(row.get("REFERENCE_MESSAGE", ""))

        slug = _slugify(question)
        task_name = slug
        task_dir = EVAL_DIR / task_name

        if task_dir.exists():
            suffix = 2
            while (EVAL_DIR / f"{task_name}-{suffix}").exists():
                suffix += 1
            task_name = f"{task_name}-{suffix}"
            task_dir = EVAL_DIR / task_name

        _write_task(task_dir, question, reference)
        task_names.append(task_name)

    # Update config.yaml while preserving existing tasks
    config_path = EVAL_DIR / "config.yaml"
    existing_paths = _existing_task_paths(config_path)
    new_paths = [f"workload-performance-optimization/{name}" for name in task_names]
    merged_paths = existing_paths + [p for p in new_paths if p not in existing_paths]

    config_yaml = f"""# Harbor configuration for workload performance optimization eval
jobs_dir: \"jobs\"

agents:
  - import_path: \"cortex_code_eval.adapters:CortexCode\"
    kwargs:
      skills: [\"{SKILL_PATH}\"]

environment:
  import_path: \"cortex_code_eval.adapters:CortexCodeEnvironment\"

tasks:
""" + "\n".join([f"  - path: \"{p}\"" for p in merged_paths]) + "\n\n" + "n_attempts: 1\n"

    config_path.write_text(config_yaml, encoding="utf-8")

    print(f"Wrote {len(task_names)} tasks to {EVAL_DIR}")


if __name__ == "__main__":
    main()
