#!/usr/bin/env python3
"""Preserve every functional case execution and its suite context in reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re


def generate_report(log_file: Path, case_file: Path, matrix_file: Path | None = None,
                    json_file: Path | None = None, plan_file: Path | None = None) -> bool:
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    start_re = re.compile(r"^---\s+([A-Z][A-Z0-9]*-[0-9]+)\s+(.+?)\s+---$")
    done_re = re.compile(r"^\[(PASS|FAIL|UNSUPPORTED|NOT_RUN)\]\s+([A-Z][A-Z0-9]*-[0-9]+)\b")
    context_re = re.compile(r"^(?:\[INFO\]\s+)?==\s+((?:topology|suite):.+?)\s+==$")
    topo_re = re.compile(r"^\[UPG-TOPO\]\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+PASS=(\d+)\s+FAIL=(\d+)\s*$")
    state_re = re.compile(r"^\[UPG-STATE\]\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$")
    detail_re = re.compile(r"^\[UPG-(CLEANUP|DIAGNOSTICS)\]\s+(\S+)\s+(\S+)\s+(\S+)\s*$")
    plan_re = re.compile(r"^\[UPG-PLAN\]\s+(\S+)\s+(\S+)\s+(\S+)\s*$")
    boundary_re = re.compile(r"^\[UPG-PLAN-(BEGIN|END)\]\s+(\d+)\s*$")
    group_context_re = re.compile(r"^topology:\s*(\S+)\s+kms-backend:\s*(\S+)\s*$")
    rows = []
    row_groups = []
    pending = {}
    topo_rows = []
    groups = []
    details = {}
    detail_counts = {}
    logged_plan = []
    boundaries = []
    current_group = None
    context = "context not recorded"
    complete = True
    try:
        with log_file.open(encoding="utf-8", errors="replace") as log:
            for raw in log:
                line = ansi.sub("", raw).strip()
                if match := context_re.match(line):
                    context = match[1]
                    group_match = group_context_re.match(context)
                    current_group = group_match.groups() if group_match else None
                    pending.clear()
                elif match := plan_re.match(line):
                    topo, backend, ids = match.groups()
                    logged_plan.append((topo, backend, ids.split(",")))
                elif match := boundary_re.match(line):
                    boundaries.append((match[1], int(match[2])))
                elif match := topo_re.match(line):
                    topo_rows.append(match.groups())
                elif match := detail_re.match(line):
                    kind, topo, backend, result = match.groups()
                    details.setdefault((topo, backend), {})[kind.lower()] = result
                    key = (topo, backend, kind)
                    detail_counts[key] = detail_counts.get(key, 0) + 1
                elif match := state_re.match(line):
                    topo, backend, old_v, new_v, result = match.groups()
                    groups.append(dict(topology=topo, backend=backend, from_version=old_v,
                                       to_version=new_v, result=result,
                                       **details.pop((topo, backend), {})))
                elif match := start_re.match(line):
                    case_id, name = match.groups()
                    pending[case_id] = len(rows)
                    rows.append([case_id, f"{name} ({context})", "RUNNING"])
                    row_groups.append(current_group)
                elif match := done_re.match(line):
                    status, case_id = match.groups()
                    index = pending.pop(case_id, None)
                    if index is None:
                        complete = False
                        rows.append([case_id, f"{case_id} ({context}; start not recorded)", status])
                        row_groups.append(current_group)
                    else:
                        rows[index][2] = status
    except FileNotFoundError:
        pass

    # Modern upgrade runs persist the selected matrix before preflight. A
    # partial log must not invent a smaller successful matrix from its prefix.
    plan = logged_plan
    modern = bool(groups or logged_plan or boundaries)
    if plan_file is None:
        candidate = log_file.parent / "plan.tsv"
        plan_file = candidate if candidate.exists() else None
    if plan_file is not None:
        modern = True
        try:
            plan = []
            for line in plan_file.read_text(encoding="utf-8").splitlines():
                topo, backend, ids = line.split("\t")
                plan.append((topo, backend, ids.split(",")))
        except (OSError, ValueError):
            plan = []
            complete = False
        if logged_plan:
            complete = complete and logged_plan == plan
            complete = complete and boundaries == [("BEGIN", len(plan)), ("END", len(plan))]
    elif modern:
        complete = complete and boundaries == [("BEGIN", len(plan)), ("END", len(plan))]
    if modern:
        identities = [(topo, backend) for topo, backend, _ in plan]
        complete = complete and bool(plan) and len(identities) == len(set(identities))
        for topo, backend, expected in plan:
            identity = (topo, backend)
            complete = complete and bool(expected) and len(expected) == len(set(expected)) and all(
                re.fullmatch(r"UPG-[0-9]+", cid) for cid in expected)
            actual = [row[0] for row, group in zip(rows, row_groups) if group == identity]
            complete = complete and sorted(actual) == sorted(expected)
            for cid in expected:
                if cid not in actual:
                    rows.append([cid, f"{cid} (topology: {topo} kms-backend: {backend}; execution not recorded)", "NOT_RUN"])
                    row_groups.append(identity)
            terminal = [group for group in groups if (group["topology"], group["backend"]) == identity]
            complete = complete and len(terminal) == 1 and all(
                detail_counts.get((topo, backend, kind)) == 1 for kind in ("CLEANUP", "DIAGNOSTICS"))
            if not terminal:
                groups.append(dict(topology=topo, backend=backend, from_version="unknown", to_version="unknown",
                                   result="INCOMPLETE", **details.get(identity, {})))
        complete = complete and all(group in identities for group in row_groups) and all(
            (group["topology"], group["backend"]) in identities for group in groups)

    counts = {status: sum(row[2] == status for row in rows)
              for status in ("PASS", "FAIL", "UNSUPPORTED", "RUNNING", "NOT_RUN")}
    with case_file.open("w", encoding="utf-8") as out:
        out.write(f"## Case Summary\n\n- Total: {len(rows)}\n")
        for status, count in counts.items():
            out.write(f"- {status}: {count}\n")
        out.write("\n| Case | Name | Status |\n| --- | --- | --- |\n")
        for row in rows:
            out.write("| " + " | ".join(value.replace("|", "&#124;") for value in row) + " |\n")
        if not rows:
            out.write("\nNo case execution was recorded; the log is missing, empty, or stopped before the cases.\n")

    valid = complete and bool(rows) and not any(counts[status] for status in ("FAIL", "RUNNING", "NOT_RUN"))
    if groups:
        valid = valid and all(group["result"] == "PASS" and group.get("cleanup") in ("PASS", "RETAINED")
                             and group.get("diagnostics") == "PASS" for group in groups)
        # Unsupported/partial upgrade cases do not prove compatibility.
        valid = valid and all(row[2] == "PASS" for row in rows)
    if matrix_file is not None:
        with matrix_file.open("w", encoding="utf-8") as out:
            out.write("## Upgrade Matrix\n\n| Topology | KMS Backend | From Version | To Version | Result | Cleanup | Diagnostics |\n")
            out.write("| --- | --- | --- | --- | --- | --- | --- |\n")
            if groups:
                for group in groups:
                    out.write("| " + " | ".join(group.get(field, "NOT_RUN") for field in
                              ("topology", "backend", "from_version", "to_version", "result", "cleanup", "diagnostics")) + " |\n")
                out.write("\nPREPARE_FAILED: 旧版环境或种子准备失败；UPGRADE_FAILED: 升级阶段失败；"
                          "VERIFY_FAILED: 升级后断言失败；NOT_RUN: 未执行；INTERRUPTED: 被中断。"
                          "INCOMPLETE: 计划中的终态证据缺失；DIAGNOSTICS_FAILED / CLEANUP_FAILED: 证据归档或环境恢复失败。\n")
            else:
                for topo, backend, old_v, new_v, npass, nfail in topo_rows:
                    result = "PASS" if nfail == "0" else "FAIL"
                    out.write(f"| {topo} | {backend} | {old_v} | {new_v} | {result} (PASS={npass} FAIL={nfail}) | unknown | unknown |\n")
            if not groups and not topo_rows:
                out.write("| - | - | - | - | NOT RUN (suite failed before upgrade) |\n")
        valid = valid and (bool(groups) or (bool(topo_rows) and all(row[-1] == "0" for row in topo_rows)))
    if json_file is not None:
        json_file.write_text(json.dumps(dict(schema=1, status="PASS" if valid else "FAIL", counts=counts,
                             cases=[dict(id=cid, name=name, status=status) for cid, name, status in rows],
                             groups=groups, plan=[dict(topology=topo, backend=backend, cases=ids) for topo, backend, ids in plan]),
                             ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return valid


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_file", type=Path)
    parser.add_argument("case_file", type=Path)
    parser.add_argument("matrix_file", type=Path, nargs="?")
    parser.add_argument("--json", dest="json_file", type=Path)
    parser.add_argument("--plan", dest="plan_file", type=Path)
    args = parser.parse_args()
    raise SystemExit(0 if generate_report(args.log_file, args.case_file, args.matrix_file, args.json_file, args.plan_file) else 1)
