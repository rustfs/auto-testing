#!/usr/bin/env python3
"""F06: cancelled Admin journal survives the same-unit full fleet restart."""

from __future__ import annotations

import sys

try:
    from .heal_probe import canonical, require
    from .healthy import physical_content
    from .cursor import main as dx_main, wanted_pairs
    from .versions import public_version_id
    from .stop import observe_task as stop_task, status, idle_fleet
except ImportError:
    from heal_probe import canonical, require
    from healthy import physical_content
    from cursor import main as dx_main, wanted_pairs
    from versions import public_version_id
    from stop import observe_task as stop_task, status, idle_fleet


CASE_ID = "heal.cancelled-task-restart"
BRANCHES = ("cancel-restart", "old-token-after-restart", "force-new-after-cancel")
REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "terminalSchema": 1, "reportSchema": 1, "intentPrefix": "root-heal-",
    "terminalPrefix": "terminal-root-heal-", "reportPrefix": "heal-terminal-report-",
    "sources": {"crates/heal/src/heal/manager/root_recovery.rs": "ba6beba9d9d14985e4c5448d85727036388d3d72f28879ac5c67fc5f760048ba",
                "crates/heal/src/heal/manager/root_recovery/report.rs": "9beca8d450e426d66315e10ea2cebf4b375719c06d9019a7263e20f559070009"},
    "role": "exact run-owned read-only journal witness; no synthesized checkpoint or record replay"}


def validate_journal(observed, token, bucket, cancelled, expected):
    wanted = wanted_pairs(expected)
    present, all_records = {}, []
    for node, value in observed.items():
        require(value.get("readOnly") is True and value.get("taskId") == token
                and isinstance(value.get("records"), list) and len(value["records"]) == 12,
                "Admin journal did not cover each actual node's four drives and three exact names")
        seen = set()
        for entry in value["records"]:
            identity = (node, entry.get("disk"), entry.get("kind"))
            require(type(identity[1]) is int and 0 <= identity[1] < 4 and identity[2] in {"intent", "terminal", "report"}
                    and identity not in seen and type(entry.get("present")) is bool, "invalid or duplicate journal witness")
            seen.add(identity)
            if entry["present"]:
                require(entry["kind"] not in present, "Admin task has duplicate physical journal owners")
                present[entry["kind"]] = (node, entry)
            all_records.append({"node": node, **entry})
    require(set(present) == {"terminal", "report"}, "cancelled task retained runnable intent or lost its durable terminal/report")
    terminal_node, terminal_entry = present["terminal"]
    report_node, report_entry = present["report"]
    require((terminal_node, terminal_entry["disk"]) == (report_node, report_entry["disk"]), "journal terminal/report owners disagree")
    terminal, report = terminal_entry["value"], report_entry["value"]
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1
            and terminal.get("task_id") == token and terminal.get("status") == "Cancelled"
            and terminal.get("heal_type") == {"type": "prefix", "bucket": bucket, "prefix": "objects/"},
            "durable terminal has the wrong task, scope, schema or cancelled state")
    require(type(report.get("schema")) is int and report["schema"] == 1 and report.get("terminal") == terminal
            and canonical(report.get("outcome")) == canonical(cancelled.get("outcome")),
            "durable report differs from the settled cancelled responsibility")
    require(type(report.get("next_seq")) is int and type(report.get("min_seq")) is int
            and 1 <= report["min_seq"] <= report["next_seq"] and isinstance(report.get("seqed_items"), list)
            and 0 < len(report["seqed_items"]) <= 1024, "durable cancelled report has invalid sequence bounds")
    previous, seen_items = 0, set()
    for item in report["seqed_items"]:
        require(isinstance(item, list) and len(item) == 2 and type(item[0]) is int
                and max(previous + 1, report["min_seq"]) <= item[0] < report["next_seq"]
                and item[1].get("bucket") == bucket
                and (item[1].get("object"), public_version_id(item[1].get("versionId"))) in wanted
                and item[1].get("objectSize") == wanted[item[1]["object"], public_version_id(item[1].get("versionId"))]["payload"]["size"],
                "durable report cursor/item crossed the old task responsibility")
        pair = item[1]["object"], public_version_id(item[1].get("versionId"))
        require(pair not in seen_items, "durable cancelled report duplicated an ACK identity")
        seen_items.add(pair)
        previous = item[0]
    return {"ownerNode": terminal_node, "ownerDisk": terminal_entry["disk"], "cancelledTaskId": token,
            "runnableIntentAbsentOnEveryDrive": True, "terminalAndReportExactlyMatch": True,
            "allRecords": all_records}


def observe_task(probe, fleet, binding, bucket, expected, targets, active, *, original):
    fleet.record("journal-source-contract", REFERENCE)
    archived = {}
    def restart(path, old, cancelled, quiet):
        fleet.stop()
        before = fleet.record("cancelled-journal-before-restart", {node: fleet.remote(node, "task-records", taskId=old,
            quiescent=True) for node in fleet.nodes})
        journal = validate_journal(before, old, bucket, cancelled, expected)
        archived.update(records=before, cancelled=cancelled, path=path, old=old)
        stopped = fleet.snapshot("cancelled-before-restart", quiescent=True)
        require(physical_content(stopped) == physical_content(quiet), "graceful stop changed cancelled object files/formats")
        fleet.start()
        idle = idle_fleet(fleet, binding)
        old_after = status(probe, fleet, path, old, "cancelled-status-after-restart")
        require(old_after.get("summary") == "stopped" and canonical(old_after.get("outcome")) == canonical(cancelled["outcome"]),
                "cancelled task revived, lost its responsibility or falsely completed after restart")
        restarted = fleet.snapshot("cancelled-live-after-restart")
        require(physical_content(restarted) == physical_content(quiet) and all(
            restarted[node]["pid"] > 0 and restarted[node]["pid"] != quiet[node]["pid"]
            and restarted[node]["unit"] == quiet[node]["unit"] and restarted[node]["binarySha256"] == quiet[node]["binarySha256"]
            for node in fleet.nodes), "restart replaced topology/candidate/roots or failed to start new processes")
        after = fleet.record("cancelled-journal-after-restart", {node: fleet.remote(node, "task-records", taskId=old)
            for node in fleet.nodes})
        validate_journal(after, old, bucket, old_after, expected)
        require(canonical(before) == canonical(after), "restart rewrote the original cancelled task's journal")
        return {"journal": journal, "sameOriginalUnitsRootsAndCandidate": True, "newProcessOnEveryNode": True,
                "oldCancelledOutcomePreserved": True, "oldJournalBytesPreserved": True, "allPeerIdle": idle}
    proof = stop_task(probe, fleet, binding, bucket, expected, targets, active, original=original,
                      after_cancelled=restart, force_new=True)
    final_old = status(probe, fleet, archived["path"], archived["old"], "old-status-after-new-completion")
    final_journal = fleet.record("old-journal-after-new-completion", {node: fleet.remote(node, "task-records", taskId=archived["old"])
        for node in fleet.nodes})
    validate_journal(final_journal, archived["old"], bucket, final_old, expected)
    require(canonical(final_journal) == canonical(archived["records"]),
            "new forceStart task rewrote or removed the original cancelled task's journal")
    return {"cancel-restart": {"verdict": "PASS", "cancellation": proof["stop-active"]},
            "old-token-after-restart": {"verdict": "PASS", "oldStatus": proof["old-status"],
                "retiredStop": proof["old-stop-new-task"]["staleStop"], "oldJournalPreservedThroughNewCompletion": True},
            "force-new-after-cancel": {"verdict": "PASS", "forceStart": True, **proof["old-stop-new-task"]}}


if __name__ == "__main__":
    try:
        dx_main(case_id=CASE_ID, observer=observe_task, branches=BRANCHES)
    except Exception as exc:
        print(f"Heal cancelled-restart validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
