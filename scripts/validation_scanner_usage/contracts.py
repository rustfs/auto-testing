"""Keep the twenty design acceptance rows distinct from black-box probes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    number: int
    slug: str
    title: str
    scenario: str | None
    assertions: tuple[tuple[str, str], ...]

    @property
    def id(self):
        return f"scanner.usage-{self.number:02d}-{self.slug}"

    @property
    def design_id(self):
        return f"USAGE-{self.number:02d}"


# Each assertion's first item is its required evidence source. Native assertions
# require an exact candidate test and emitted assertion evidence, not a shell exit.
CASES = (
    Case(1, "static-accounting", "Static creation, writes, deletion and empty buckets", "static", (
        ("public", "Committed objects and an independent paginated S3 inventory have identical keys and bytes."),
        ("public", "Usage converges to exact object counts and logical bytes after writes and deletes."),
        ("public", "A measured empty bucket is distinct from an absent or unknown bucket value."))),
    Case(2, "unrelated-remote-mutations", "Long sweep with unrelated remote mutations", None, (
        ("native", "A long bucket makes durable progress across at least three cycles while another node mutates an unrelated bucket."),
        ("native", "The complete producer watch proof prevents unrelated writes from invalidating that bucket's certification."))),
    Case(3, "cross-cycle-mutations", "Mutations of an already scanned range", "mutations", (
        ("public", "Acknowledged insert, overwrite and delete operations converge to an independent final S3 inventory."),
        ("native", "A mutation occurs after the affected range was scanned and before a later cycle whose start and post generations agree."),
        ("native", "The mixed result is not certified or acknowledged, and a later fresh sweep is exact."))),
    Case(4, "hot-prefix-progress", "Hot and cold ranges in one bucket", "mutations", (
        ("public", "Nested and prefix-shaped keys retain exact final contents after controlled mutations stop."),
        ("native", "The cold range obtains durable traversal progress while a hot prefix is continuously modified."),
        ("native", "A whole-bucket observation never claims independent prefix certification or prefix ACK."))),
    Case(5, "shared-budget-fairness", "One large bucket and three cold buckets", "multi-bucket", (
        ("public", "Three cold buckets converge exactly while requests continue against a separate hot bucket."),
        ("native", "A real shared cycle budget is exhausted without a bucket quantum cancelling its parent."),
        ("native", "All cold buckets receive service and a zero-progress turn cannot busy-loop."))),
    Case(6, "wide-deep-directories", "Wide and deep directory progress", None, (
        ("native", "A prefix containing at least 100000 entries and nested wide directories advances its durable frontier."),
        ("native", "Checkpoint memory and bytes remain bounded across rounds without sacrificing exact counts."))),
    Case(7, "multi-source-coverage", "Coverage across pools and sets", "multi-endpoint", (
        ("public", "Acknowledged writes through every configured endpoint remain readable and converge to the same exact bucket usage."),
        ("native", "The fixture has more physical sources than the set concurrency limit and every expected source contributes."),
        ("native", "A missing or non-fresh source prevents complete totals and whole-bucket ACK."))),
    Case(8, "no-baseline", "Cold startup and a new bucket without LKG", "no-baseline", (
        ("public", "An absent bucket value stays unknown in the test oracle until a real measured value is available."),
        ("native", "A cold-start partial or mixed bucket has no legal LKG and cannot enter the legacy primary map or quota fallback."))),
    Case(9, "value-freshness", "Carried values and complete mixed observations", "freshness", (
        ("public", "Returned carried or observed values obey the per-bucket certification contract without using last_update as a certificate."),
        ("native", "Multiple PartialCarried and mixed rounds preserve the original value certificate or observation completion time."),
        ("native", "A mixture of current and old source values does not inherit an unrelated whole-bucket certificate."))),
    Case(10, "checkpoint-recovery", "Checkpoint and mixed EOF crash recovery", None, (
        ("native", "Crashes before and after mixed EOF CAS keep the observation, cleared cursor and fresh-sweep debt atomic."),
        ("native", "A successor rejects late old-leader writes and never clears responsibility after losing an observation."))),
    Case(11, "bucket-recreation", "Same-name bucket recreation", "recreation", (
        ("public", "Deleting and recreating the suite-owned bucket converges to the successor's exact keys and bytes."),
        ("native", "An old incarnation cannot contribute to carry, overlay, certification or ACK after recreation."))),
    Case(12, "watch-proof", "Watch capability, gaps, replay and ABA", None, (
        ("native", "A missing producer watch proof stays unknown and retains the strict mutation fallback."),
        ("native", "A peer without watch capability cannot authorize cross-cycle reuse."),
        ("native", "A mutation racing watch installation cannot establish a falsely clean interval."),
        ("native", "Watch capacity overflow invalidates proof instead of retaining a clean revision."),
        ("native", "An expired watch cannot prove an unchanged bucket."),
        ("native", "A producer restart invalidates watches from its previous incarnation."),
        ("native", "A stale or replayed watch response cannot authorize the current request."),
        ("native", "An ACK ABA race cannot resurrect an old clean watch or clear newer work."))),
    Case(13, "publication-failures", "Lease, CAS and deadline publication failures", None, (
        ("native", "Object and directory count stops share one drain deadline that does not restart after a later budget or lease phase."),
        ("native", "A bucket quantum's remote request duration and work limits never exceed its parent's remaining limits."),
        ("native", "A remote completion received during the bounded drain retains the owning cycle and reaches the publication decision."),
        ("native", "Losing the lease or epoch during drain rejects publication and cannot restart the drain deadline."),
        ("native", "A failed canonical root CAS cannot authorize ACK."),
        ("native", "A failed companion persistence operation cannot authorize ACK or clear pending responsibility."),
        ("native", "A sealed publication candidate cannot be changed by late collector results."),
        ("native", "A successful durable canonical root for the still-owned epoch permits only its all-source-fresh bucket ACKs."))),
    Case(14, "ack-races", "Duplicate, lost and reordered acknowledgements", None, (
        ("native", "Duplicate, lost or reordered ACK cannot clear a newer generation or a post-walk mutation."),
        ("native", "Only a durable exact canonical root permits all-source-fresh bucket ACK; pending never falls back to a whole-generation ACK."))),
    Case(15, "structural-changes", "Movement, topology and tier changes", None, (
        ("native", "Movement, expansion, decommission and tier-generation changes reject obsolete source and structure identities."),
        ("native", "Structural changes cannot bypass publication fences or permit stale overlay and certification."))),
    Case(16, "mixed-version", "Mixed readers, workers, peers and rollback", None, (
        ("native", "An old reader cannot treat a new mixed observation as an LKG primary value."),
        ("native", "Terminal, progress and watch capabilities negotiate independently and rolling rollback preserves system data and responsibility."))),
    Case(17, "load-and-convergence", "Sixty-minute workload and stop-write convergence", "load", (
        ("public", "A configured sixty-minute sequential workload retains per-request timings and available service error codes before exact stop-write convergence."),
        ("native", "Matched base and candidate load runs retain comparable throughput, latency, observed HTTP 503 counts, per-bucket freshness and binary identities."))),
    Case(18, "diagnostic-boundaries", "Metrics and admin projection boundaries", "diagnostics", (
        ("public", "Admin samples retain collection times separately from returned per-bucket certification times and reject malformed metadata."),
        ("native", "Collector delay or failure cannot invent a per-bucket measurement or certification time."),
        ("native", "The admin-only companion overlay cannot flow into quota, cache or durable writer inputs."))),
    Case(19, "quota-compatibility", "Quota and legacy snapshot compatibility", None, (
        ("native", "No-quota, durable quota v1/v2 and legacy snapshot paths retain their original admission decisions."),
        ("native", "Companion observations cannot expand legacy quota fallback or durable-ledger admission."))),
    Case(20, "versions-and-prefix-keys", "Object/prefix collisions, versions and markers", "versions", (
        ("public", "An object key that is also a prefix and nested encoding fixtures retain every acknowledged version and payload."),
        ("public", "Usage matches the independent version inventory's object, version, marker and logical-byte accounting exactly."))),
)


def assertion_id(case: Case, index: int) -> str:
    return f"{case.design_id}-A{index + 1:02d}"


def select_cases(value: str) -> list[Case]:
    if value == "all":
        return list(CASES)
    if value == "blackbox":
        return [case for case in CASES if all(kind == "public" for kind, _ in case.assertions)]
    names = value.split(",")
    selected = [case for case in CASES if case.id in names or case.design_id in names]
    if not selected or len(selected) != len(names):
        raise ValueError("unknown or duplicate scanner usage case selection")
    return selected


def catalog_module() -> dict:
    return {"id": "scanner-usage-regressions", "origin": "auto-testing",
            "target_environments": ["configured-endpoint", "native-test"],
            "required_capabilities": ["explicit-endpoint", "candidate-provenance"],
            "cases": [{"id": case.id, "source_status": "procedure-ready", "priority": "P1",
                       "channels": ["admin-api", "s3-api", "native-test"],
                       "assertions": [text for _, text in case.assertions],
                       "probes": {"designCode": case.design_id,
                                  "procedure": "docs/scanner-usage-validation.md",
                                  "executionMode": "configured-endpoint",
                                  "runner": "scripts/validation_scanner_usage/run_case.sh",
                                  "publicScenario": case.scenario},
                       "reuse_candidates": {"suites": ["rustfs-scanner-usage-test.sh"], "case_ids": []}}
                      for case in CASES]}
