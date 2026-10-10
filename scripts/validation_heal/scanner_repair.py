"""E04: cursor-bounded scanner discovery, source admission and exact task proof."""

from datetime import datetime, timezone
import json
import time
from urllib.parse import quote

try:
    from .heal_probe import Probe, require
except ImportError:
    from heal_probe import Probe, require


BRANCHES = ("cold-missing", "cold-bitrot", "scanner-cycle-boundary")
BUDGET_SECONDS = 240
SOURCE_TARGETS = {"rustfs::scanner", "rustfs::scanner::folder", "rustfs::heal::channel",
                  "rustfs::heal::manager", "rustfs::heal::task"}


def event_time(event):
    value = event.get("timestamp")
    if not isinstance(value, str): return None
    try: stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError: return None
    return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None


def null_version(value):
    return value in {None, "", "null", "None", "00000000-0000-0000-0000-000000000000"}


class JournalWindow:
    def __init__(self, fleet, label, *, event_targets=None):
        self.fleet, self.label = fleet, label
        self.event_targets = event_targets
        self.events = {node: [] for node in fleet.nodes}
        self.cursors = {}
        for node in fleet.nodes:
            observed = fleet.record(label + "-checkpoint", {"node": node, **fleet.remote(node, "logs", checkpoint=True)})
            require(observed.get("complete") is True and observed.get("afterCursor"), "missing source journal checkpoint")
            self.cursors[node] = observed["afterCursor"]

    def capture(self):
        for node in self.fleet.nodes:
            for page in range(6):
                observed = self.fleet.record(self.label + "-window", {"node": node,
                    **self.fleet.remote(node, "logs", afterCursor=self.cursors[node])})
                require(observed.get("complete") is True and observed.get("beforeCursor") == self.cursors[node]
                        and observed.get("afterCursor"), "journal window lost its checkpoint or completeness")
                require(observed.get("hasMore") is not True or observed["afterCursor"] != self.cursors[node],
                        "journal pagination did not advance")
                self.cursors[node] = observed["afterCursor"]
                for line in observed.get("journal", "").splitlines():
                    try: event = json.loads(line)
                    except json.JSONDecodeError: continue
                    # Raw pages remain in immutable evidence; keep only source
                    # attribution events in memory, not unrelated debug noise.
                    if isinstance(event, dict) and (self.event_targets is None
                            or event.get("target") in self.event_targets):
                        self.events[node].append(event)
                if observed.get("hasMore") is not True: break
            else:
                raise ValueError("journal window exceeds its page budget; source attribution is incomplete")
            require(len(self.events[node]) <= 60000, "scanner evidence exceeded its registered event budget")
        return self.events


def cycle_pairs(events):
    """Pair actual timestamps on one leader; completed cycle may serialize as 0."""
    pairs, pending = [], None
    for event in events:
        if event.get("target") != "rustfs::scanner" or event.get("event") != "scanner_cycle_state": continue
        stamp = event_time(event)
        if stamp is None:
            pending = None
            continue
        state, cycle = event.get("state"), event.get("cycle")
        if state == "started":
            pending = (cycle, stamp) if type(cycle) is int and cycle > 0 and event.get("scan_mode") == "Deep" else None
        elif state == "completed":
            if pending and type(cycle) is int and cycle in (0, pending[0]) and stamp > pending[1]:
                pairs.append({"cycle": pending[0], "started": pending[1], "completed": stamp})
            pending = None
        elif state in {"failed", "aborted", "budget_exhausted", "deferred"}:
            pending = None
    return pairs


def source_tasks(events, bucket, entry):
    """Bind discovery -> channel -> accepted Scanner source -> exact object task."""
    require(entry["versionId"] == "null" and not entry["deleteMarker"], "scanner cold fixture must be unversioned data")
    found = []
    for node, rows in events.items():
        pairs = cycle_pairs(rows)
        discoveries = [event for event in rows if event.get("target") == "rustfs::scanner::folder"
            and event.get("event") == "scanner_heal_admission" and event.get("state") == "request_started"
            and event.get("bucket") == bucket and event.get("object") == entry["key"]
            and "version_id" in event and null_version(event["version_id"]) and event_time(event) is not None]
        for start in rows:
            if not (start.get("target") == "rustfs::heal::task" and start.get("message") == "Heal object started"
                    and start.get("stage") == "start" and start.get("bucket") == bucket
                    and start.get("object") == entry["key"] and "version_id" in start
                    and null_version(start["version_id"])): continue
            token, stamp = start.get("task_id"), event_time(start)
            if not isinstance(token, str) or not token or stamp is None: continue
            received = [event for event in rows if event.get("target") == "rustfs::heal::channel"
                and event.get("state") == "start_received" and event.get("request_id") == token
                and event.get("bucket") == bucket and event.get("object_prefix") == entry["key"]]
            accepted = [event for event in rows if event.get("target") == "rustfs::heal::manager"
                and event.get("message") == "Heal queue request accepted" and event.get("request_id") == token
                and event.get("source") == "scanner" and event.get("result") == "accepted"]
            completed = [event for event in rows if event.get("target") == "rustfs::heal::manager"
                and event.get("state") == "task_completed" and event.get("task_id") == token]
            for receipt in received:
                receipt_time = event_time(receipt)
                if receipt_time is None: continue
                discovery = next((event for event in reversed(discoveries) if event_time(event) <= receipt_time), None)
                if discovery is None: continue
                admission = next((event for event in accepted if event_time(event) is not None
                    and receipt_time <= event_time(event) <= stamp), None)
                done = next((event for event in completed if event_time(event) is not None and stamp <= event_time(event)), None)
                cycle = next((pair for pair in pairs if pair["started"] <= event_time(discovery)
                    <= pair["completed"] and event_time(discovery) <= stamp), None)
                if admission is not None and done is not None and cycle is not None:
                    found.append({"node": node, "taskId": token, "discovery": discovery, "received": receipt,
                        "admission": admission, "started": start, "completed": done,
                        "cycle": {name: value.isoformat() if isinstance(value, datetime) else value for name, value in cycle.items()}})
                    break
    return found


def observe_task(probe, fleet, window, bucket, entry, branch, *, healthy=False):
    require(fleet.runtime_profile == "scanner", "scanner proof requires its isolated runtime profile")
    deadline = time.monotonic() + BUDGET_SECONDS
    while True:
        events = window.capture()
        tasks = source_tasks(events, bucket, entry)
        if tasks:
            if branch != "scanner-cycle-boundary" or healthy or any(len(cycle_pairs(events[task["node"]])) >= 2 for task in tasks):
                break
        require(time.monotonic() < deadline, "ATTRIBUTION_NOT_OBSERVED: scanner did not discover, admit and complete the exact target in its cycle")
        time.sleep(1)
    # A later healthy scanner pass cannot certify the injected fault. Inspect
    # tasks in order and select the first exact repaired receipt for this run.
    observations = []
    for task in tasks:
        endpoint = "node" + str(fleet.nodes.index(task["node"]) + 1)
        observer = Probe(probe.binding, endpoint)
        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(entry["key"], safe="/")
        observed = fleet.record("scanner-task-status", observer.admin(path, [("clientToken", task["taskId"])], b""))
        require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                "scanner task cannot be queried by its exact source identity")
        status = observed["response"]
        observations.append({"source": task, "status": status})
        objects = status.get("outcome", {}).get("objects", [])
        if len(objects) == 1 and objects[0].get("disposition", {}).get("state") == ("verified_healthy" if healthy else "repaired"):
            fleet.record("scanner-source-binding", {"source": "scanner", "branch": branch, "healthyPreflight": healthy,
                "budgetSeconds": BUDGET_SECONDS, "task": task, "targetReadsBeforePhysicalProof": 0,
                "adminMutations": 0, "observedTasks": observations,
                "postFaultCycles": len(cycle_pairs(events[task["node"]]))})
            return status
    raise ValueError("scanner tasks did not provide the required exact healthy/repaired responsibility; inspect source evidence")
