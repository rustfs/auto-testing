# Heal Live Execution Manual: Common Commands for the 84 Procedure Cards

This manual, together with the [84 per-case procedure cards](validation-heal-cases.md), forms an executable **manual procedure plus script assistance**. It is not 84 unattended automated tests, and it is not a pass report. The procedure cards list 302 branch templates; actual disk roles, thresholds, damage counts, and topologies still have to be expanded in each batch's `parameterBindings`. The template count must not be treated as the number of live measurements.

The common acceptance contract is in the [Heal specification](validation-heal-contract.md). Historical pilot results from the source repository are not counted in the current canonical denominator. The automatic entry point `scripts/run_validation_case.sh CASE_ID CANDIDATE_MANIFEST TESTING_COMMIT ARTIFACTS_DIR` already covers 67 Heal cases. The complete list is viewed with `matrix.py list`, and `automaticExecutor` is authoritative. The entry point uses the current Heal nodes, the candidate deb, and independent H1/H2 services; the remaining procedure cards still have to be carried out, with evidence retained, according to this manual.

## 1. Entry points and deliverables

```bash
python scripts/validation_heal/matrix.py list
python scripts/validation_heal/matrix.py show C02
python scripts/validation_heal/matrix.py show F02,F03,H07
python scripts/validation_heal/matrix.py plan --selection all \
  --run-id manual-heal-20260910-a --out reports/validation-heal-84-plan.json
python scripts/validation_heal/matrix.py plan --selection A01,A04,C01,C02,C04,J04 \
  --topologies H1 --run-id manual-heal-20260910-b --out reports/heal-h1-plan.json
```

`plan` only freezes the selection, the branches, and the corresponding operations. It does not deploy, and it does not score. `show` also gives the steps, the command entry points, the acceptance criteria, and the recovery requirements. Different topologies are different instances; an unselected topology retains `scopeExclusions`. A partial result must not be claimed as a full-matrix pass.

Automatic cases are dispatched through the RustFS Functional Chain `validation_case` and the exact `auto_testing_sha`. First build the read-only metadata inspector from the same immutable candidate's `build_run_id`/`build_run_attempt` with `validation_tools_only=true`. Live dispatch supplies that successful build's `validation_tools_run_id` and `validation_tools_run_attempt`. The tool's candidate/helper SHA, Linux binary SHA, and independent Cargo.lock digest must all pass their checks before they are copied onto this case's nodes. Ordinary live execution consumes the build artifacts and does not need a RustFS checkout.

| Tool | Implemented responsibility | What it does not represent |
| --- | --- | --- |
| `matrix.py` | Index of the 84 items and 302 branches, per-case procedure cards, frozen plans, and evidence-completeness audits | It does not assign topologies automatically, and it does not judge PASS merely because a file exists |
| `heal_probe.py` | Raw Admin with a signature, anonymous access, or a bad signature; restricted S3 operations; full-version, tag, and content-digest inventories and comparison | An HTTP check passing does not mean heal succeeded; an inventory GET triggers read-repair |
| `heal_workload.py` | Real DB/DV/DM/DS/DX fixtures; bounded concurrency, serial foreground mutation of the same key, and durable persistence before the request and after the ACK | It does not derive the post-race final version for the user; a timed-out write must be resolved separately |
| `heal_fault.py` | Linux descriptor-relative addressing, mount-ID protection, single-file missing/bitflip/truncate/zero/header, snapshots, and recovery | A file fault is not a physical disk loss; it does not modify system metadata; it does not blindly restore a new generation |
| `prepare_heal_meta_inspector.py` | Prepares a read-only Rust metadata parser project for the exact candidate source | It must be compiled on an approved build machine; parsing metadata is not verification of bitrot or EC |
| `shards.py` | All 24 branches of C01–C04, D02, D07, E01, and E02, plus A03's two H2 cross-node state branches and A04's six DB/DV dry-run branches; quiescent faults, real roles, same-role original shards and HighwayHash, physical checks before GET/Range, and MPU control | An automatic entry point does not mean the candidate passed; a task that is unknown, a target that only restores metadata, or a rescued backup cannot be recorded as PASS |
| `check_bitrot_tool.py` | Checks 24 healthy shards and 12 bad-shard negative cases against fixed MinIO and historical RustFS artifacts | Tool verification is not a live case pass; it does not call the RustFS service under test |

## 2. Bind the environment first; do not guess nodes directly

Live commands use the current auto-testing `smoke-testing` runner, `RUSTFS_NODES`, and `RUSTFS_SSH_USER`, and they reuse the Heal nodes of `rustfs_heal_test.sh`. Before a run, acquire the shared node concurrency lock that matches the Functional Chain. H1–H6 are test-topology requirements, not another set of SSH hosts. Machines must not be chosen by historical hostnames or IP addresses from the source repository.

### Read-only diagnosis when the runner cannot accept work

The RustFS Functional Chain provides the `validation_runner_inspect_only=true` entry point. On `ubuntu-latest` it reads GitHub runner registration information and active jobs, uses an independent concurrency group, and neither occupies nor replaces the test fleet queue. A single request may select only this mode, and must not also pass candidate, tools, case, or recovery parameters. Use a reviewed workflow ref that contains this entry point:

```bash
gh workflow run rustfs-functional-chain.yml --repo rustfs/rustfs \
  --ref "$VALIDATION_DRIVER_REF" -f validation_runner_inspect_only=true
```

Download that run's `validation-runner-inspection-<runId>-<attempt>` artifact and check `workflowCommit`, `runId`, `runAttempt`, `observedAt`, `complete`, and `runnerAvailability`. `offline`, `online-busy`, `online-idle`, and `unregistered` are distinct observations of GitHub registration state. An API rejection, pagination past the limit, or any other error retains `complete=false` and the failure stage, and must not be interpreted as idle. The report does not connect to nodes, does not restart services, does not clean up files, and does not cancel workflows, and it does not prove that the environment has recovered or that the shared workflow is globally idle. After the runner service recovers, the resource checks and the original scene and concurrency gates still have to be executed again.

Use `matrix.py plan` to freeze the current SSH nodes and every applicable operation branch, then fill in the private `lab.json` from this read-only preflight. `verified:false` in the plan must not be rewritten directly as a passed resource verification. H4 requires at least 24 allocated independent disk endpoints. H6 requires an exclusive host and bindings for platform events and recovery. A branch that lacks resources remains BLOCKED.

The filled-in contents must include:

- This batch's run ID, deadline, exclusive service and port, jump route, actual hostnames, each disk's mount UUID and major:minor, and the service unit, original PID, and data root;
- The immutable candidate manifest, source commit, and package and installed-binary SHA-256 that match the current validation lane; the current rc path and binary SHA-256; the auto-testing commit; save the real build inventory, and do not fill in only a digest string;
- The actual shape of H1–H6, each object's N/data/parity/R/W/lock quorum, and pool/set/slot/disk UUID; H4 has at least 24 actual disk endpoints;
- The maximum repair, restart, and fault time limits, the object, byte, request, and concurrency budgets, and the exact target and recovery owner of every dangerous action;
- For an instance that includes platform operations, an exclusive fault device, an old version, KMS/tier, or a native barrier, first provide capability and authorization evidence.

`binding-check` is only a syntax and lease check. It is not a remote proof of resource or package identity:

```bash
python scripts/validation_heal/heal_probe.py --binding lab.json --out reports/binding-check.json binding-check
```

First, on the jump host, read-only verify `hostname`, `findmnt -J`, `lsblk -J`, `ss -lnt`, the binary `sha256sum` and `--version`, the actually running configuration, and the PID. Save the output as the preflight. When the lease has not been issued, the topology does not match, a port is occupied, or a disk UUID has drifted, record BLOCKED here. Do not inject first and skip afterward.

### Deployment of the independent experiment service

H1 uses the four verified independent mounts on the current Heal nodes, and **additionally creates** `/data/rustfsN/heal-validation/<runId>/data` and a sibling `quarantine`, root-owned mode 0700. Each `<runId>` root is written with a root-owned `owner.json` that contains `runId`. Ordinary object faults may use the run-private directories under these independent parent disks. B03 and B04 also require that both the replacement target and the sibling endpoints in the same group can obtain an exact mount lease recognized by the candidate. During replacement it must be proved that the old mount and the quarantine are unreadable to the service, that after the new empty device is mounted at the original endpoint the candidate obtains a new mount lease, and that the mount ID, physical device ID, and filesystem identity are recorded before and after. A bind mount on an independent filesystem, or in an isolated mount namespace, may be used as an implementation, but creating a bind mount alone does not meet the requirement: the adapter must also switch the replaced endpoint safely, and it must pass the candidate's checks of the root and sibling physical-device identities. Before `stop_existing` and before the candidate deb is downloaded or installed, the Functional Chain calls the read-only `replacement-admission-preflight`, covering the H1/H2 endpoints. On failure it retains the package and client provenance and the complete scene readings as `BLOCKED`. It does not stop services, does not touch the installed package, and does not create a remote run namespace. `Fleet.deploy()` repeats that check again before the real reservation. When a real endpoint mount or a reviewed mount-switch adapter is missing, the result should be BLOCKED. The current runner uses only a run-private subdirectory as the endpoint and does not yet have this mount-switch adapter, so its existing four independent parent disks cannot prove that B03 and B04 meet the resource conditions. Do not extend the wait, and do not copy quarantine data. First check that each root does not exist. An existing root must not be overwritten. Reuse the candidate deb download, SHA verification, and install flow from `rustfs_heal_test.sh`, and place the installed, digest-verified `/usr/bin/rustfs` under this batch's state. Executing a case does not require a RustFS source checkout. Credential files are mode 0600. The dedicated unit below still runs on the current Heal nodes and uses the allocated test port 9000:

If the first gate passes and the reservation-time recheck in `Fleet.deploy()` then finds mount drift, still record `BLOCKED` and retain the installed candidate's binary provenance. That result must not be rewritten as a product `FAIL`, and it must not be disguised as the pre-deployment `NOT_REQUIRED_NO_MUTATION`. Both stages of the check must skip reservation and fixture seeding, and must complete cleanup.

```bash
# On the target node; every variable comes from the reviewed lab binding. Arbitrary online input is not accepted.
sudo systemd-run --unit="$HEAL_UNIT" --property=Type=exec \
  --property=PrivateMounts=yes --property=KillMode=control-group \
  --property=TimeoutStopSec=30 --property=RuntimeMaxSec=3600 \
  --property=MemoryMax=2G --property=CPUQuota=200% \
  --property=WorkingDirectory="$HEAL_STATE" \
  --property=EnvironmentFile="$HEAL_STATE/service.env" \
  "$HEAL_STATE/rustfs" "${HEAL_DATA_ROOTS[@]}"
```

A scenario that needs a restart retains the unit definition, the env, the binary, and the original endpoint arguments, and does not use `--collect`. RuntimeMaxSec is frozen to that batch's budget. J05 must be assigned its own long-running service. `HEAL_DATA_ROOTS` is a reviewed Bash array and is not eval'd from an unreviewed string. H2/H3/H4/H5 use an endpoint sequence, or an explicit pool-grouping configuration, that is consistent on every node. After startup, read the result back from each disk's `format.json`. Topology success must not be guessed from the startup arguments. Do not directly run a deploy or reset command that would clean old Pool roots.

### Command conventions

The following functions run on the **jump host**. `HEAL_BINDING` and `HEAL_REPORT` are private absolute paths for this instance. `B` is a bucket under the binding prefix, `K` is the logical key, and `V` is the exact version of a previous ACK. Each command's label is unique, and existing evidence refuses overwrite. Credentials are injected only through the controlled environment, as `HEAL_ACCESS_KEY`, `HEAL_SECRET_KEY`, and an optional session token. `set -x` is disabled.

```bash
set -euo pipefail
umask 077
hp() {
  local label=$1
  shift
  python scripts/validation_heal/heal_probe.py --binding "$HEAL_BINDING" \
    --out "$HEAL_REPORT/$label.json" "$@"
}
hw() {
  local label=$1
  shift
  python scripts/validation_heal/heal_workload.py --binding "$HEAL_BINDING" \
    --out "$HEAL_REPORT/$label" "$@"
}
B=$(jq -er .bucketPrefix "$HEAL_BINDING")
RUN=$(jq -er .runId "$HEAL_BINDING")
```

Privileged fault commands run on the **target node reached through the jump host**. `ROOT` is this case's `<runId>` root on the target disk. `NODE` is the bound SSH alias. Place that SHA's `heal_fault.py` under this batch's root-owned state and check the digest. Do not assemble an SSH shell from a logical key. The following `hf` is defined only in an interactive shell on that target node:

```bash
hf() { sudo python3 "$HEAL_STATE/heal_fault.py" --root "$ROOT" --run-id "$RUN" "$@"; }
hf init --bucket-prefix "$B" --lease-expires-at "$LEASE_EXPIRES_AT"
```

Initialization only adds the fault-ownership marker. It does not create or format a data disk. Each disk is initialized only once. When a system restart changes the mount ID, pause injection. First recheck the UUID, major:minor, and ownership, and rebind manually. Do not bypass the check.

## 3. Data and client commands

<a id="SEED_DB"></a>
<a id="SEED_DV"></a>
<a id="SEED_DM"></a>
<a id="SEED_DS"></a>
<a id="SEED_DX"></a>

### Deterministic fixtures

Each command uses a new bucket. Do not seed an existing fixture bucket again and then call it the same round.

```bash
hw seed-db seed --fixture DB --bucket "$B-db"
hw seed-dv seed --fixture DV --bucket "$B-dv"
hw seed-dm seed --fixture DM --bucket "$B-dm"
hw seed-ds seed --fixture DS --bucket "$B-ds"
hw seed-dx seed --fixture DX --bucket "$B-dx"
# Replace with the real inline/block boundary confirmed by the candidate; an additional bucket may be generated:
hw seed-boundary seed --fixture DB --bucket "$B-boundary" \
  --count 128 --sizes "$BOUNDARY_SIZES"
```

DB defaults to 128 items, including 0/1/4095/4096/4097, 1MiB±1, and multi-MiB sizes, of non-repeating SHAKE data. 1MiB is not a preset product threshold. DV defaults to 4 keys, with three versions per key, alternately creating the current marker. DM has one completed three-part object (the first two parts are 5MiB+17 and 5MiB+93, and the last part is 1MiB+47) and two independent pending uploads. DS defaults to 257 special/multi-prefix keys. DX defaults to 257 external objects. Automatic seeding does not include extra branches such as suspended/null, special compression, or SSE/retention. Supplement those with the S3 commands below and save the ACKs.

Take the real UploadId, VersionId, and part ETag from the ACK file. Do not reuse a guessed ID across uploads. The total byte budget includes every version and part. When it is insufficient, first extend the approved budget. Do not skip objects automatically.

In later commands, `B` must be changed to this instance's actual bucket, for example `B="$(jq -er .bucketPrefix "$HEAL_BINDING")-db"`. Do not inventory a bare prefix that was never created. The root-directory owner stays bound to the original run prefix, so that each suffixed bucket of this run is allowed.

After seeding completes, the deterministic original content and the creation ACKs are reconciled automatically against a complete GET and version inventory, and `expected-fixture.json` and `baseline.json` are saved. Injection is not allowed until the baseline check has passed. This GET happens before the fault. The seed command must not be used to observe the scanner or read-repair fault window.

<a id="S3"></a>
<a id="GET"></a>

### Individual S3 operations and exact versions

```bash
hp put s3 --operation put_object --size 5243003 --seed "$RUN/new/1" \
  --params "$(jq -nc --arg b "$B" --arg k "$K" '{Bucket:$b,Key:$k,ContentType:"application/octet-stream",Metadata:{nonce:"1"}}')"
hp get s3 --operation get_object \
  --params "$(jq -nc --arg b "$B" --arg k "$K" --arg v "$V" '{Bucket:$b,Key:$k,VersionId:$v}')"
hp suspend s3 --operation put_bucket_versioning \
  --params "$(jq -nc --arg b "$B" '{Bucket:$b,VersioningConfiguration:{Status:"Suspended"}}')"
hp delete-version s3 --operation delete_object --expect-http 204 \
  --params "$(jq -nc --arg b "$B" --arg k "$K" --arg v "$V" '{Bucket:$b,Key:$k,VersionId:$v}')"
```

When a request is complex, `--params-file` uses JSON. Interface parameters are boto3 snake_case operation names and native CamelCase parameters. Only this run's buckets and restricted operations are allowed, and a copy source must also be in this run. GET saves only the hash and length of the complete content, and does not write object content into the log. A PUT or Complete timeout is recorded as resultUnknown. The next step resolves it by querying the same unique key/version/UploadId, with no silent retry.

`--expect-http` must use the exact value pre-registered for that request. For example, create, put, and list are commonly 200, delete and abort are commonly 204, and Range is 206. Negative cases are 304, 403, 404, 405, 412, and 416 respectively, according to the candidate contract. A CLI exit of 0 only means that this expected HTTP status matched. It never declares the whole case PASS. An SSE-C parameter value may use the literal `$HEAL_SSE_C_KEY`. The script substitutes it only in memory, and the output is redacted.

<a id="BASELINE"></a>
<a id="VERIFY"></a>

### Full-version baseline and final check

```bash
hp baseline inventory --bucket "$B"
# Finish trigger attribution and PHYSICAL first, then call the GET-type checks below:
hp post-heal inventory --bucket "$B"
hp s3-comparison compare --before "$HEAL_REPORT/baseline.json" \
  --after "$HEAL_REPORT/post-heal.json"
```

The inventory pages through every version and marker, GETs the full content, tags, and public metadata for each versionId, and then cross-checks them with a paged current LIST. The object limit is 10,000 and a single object is limited to 64MiB. The batch byte budget comes from the binding file. A larger-object scenario must pre-register a streaming tool and a budget of its own. A truncated result must not be called a complete inventory.

For concurrency, lifecycle, and legitimate-deletion scenarios, the expected inventory changes. A person first generates `expected-post.json` from the causal relationship of the per-request ACKs, recording the ACK file that corresponds to each addition or deletion, and then compares the actual inventory. Failure must not be judged directly from the static baseline, and the actual output must not be copied into the expectation. An unresolved unknown write cannot pass. Encrypted or archived objects are evidenced version by version first, using authorized GET or restore parameters. The default inventory does not automatically unlock an object, supply SSE-C, or restore. A restore-type GET must save the correspondence between the ciphertext and plaintext layers.

<a id="RANGE"></a>

### Range, HEAD, and conditional requests

```bash
hp head s3 --operation head_object --params-file request-head.json
hp range s3 --operation get_object --expect-http 206 --params-file request-range.json
hp if-none-match s3 --operation get_object --expect-http 304 --params-file request-if-none-match.json
hp if-match-reject s3 --operation get_object --expect-http 412 --params-file request-if-match-bad.json
hp range-outside s3 --operation get_object --expect-http 416 --params-file request-range-outside.json
```

Every JSON object contains this case's Bucket, Key, and optional VersionId. In addition, add respectively `Range:"bytes=5242879-5242882"`, `IfNoneMatch:<original ETag>`, `IfMatch:<an ETag that is wrong but syntactically valid>`, and a Range past the object length. Time conditions use ISO dates for `IfModifiedSince` and `IfUnmodifiedSince`. Derive one earlier and one later control value from the real LastModified returned by HEAD. Verify ContentRange, the response length, and the fragment hash computed from the deterministic original content. Do not verify only the status code.

<a id="SEED_DE"></a>
<a id="SEED_DO"></a>
<a id="DEPENDENCY"></a>

### SSE, Object Lock, and external dependencies

```bash
hp sse-s3-put s3 --operation put_object --size 5243003 --seed "$RUN/sse" --params-file sse-s3-put.json
hp sse-kms-put s3 --operation put_object --size 5243003 --seed "$RUN/kms" --params-file sse-kms-put.json
hp sse-c-put s3 --operation put_object --size 5243003 --seed "$RUN/ssec" --params-file sse-c-put.json
hp locked-bucket s3 --operation create_bucket --params-file locked-bucket.json
hp retained-put s3 --operation put_object --size 5243003 --seed "$RUN/lock" --params-file retained-put.json
hp retention s3 --operation get_object_retention --params-file protected-version.json
hp hold s3 --operation get_object_legal_hold --params-file protected-version.json
hp reject-delete s3 --operation delete_object --expect-http 403 --params-file protected-version.json
```

The SSE JSON adds, respectively, `ServerSideEncryption:"AES256"`, `ServerSideEncryption:"aws:kms",SSEKMSKeyId:<key dedicated to this case>`, and `SSECustomerAlgorithm:"AES256",SSECustomerKey:"$HEAL_SSE_C_KEY"`. An SSE-C GET also supplies the same key. First prove that the object can be decrypted with the original request. Every external key and endpoint is an approved test tenant. Do not reuse a production dependency.

The lock-bucket JSON is `{Bucket:...,ObjectLockEnabledForBucket:true}`. A protected PUT uses `ObjectLockMode:"GOVERNANCE"` or `"COMPLIANCE"`, an actual future UTC `ObjectLockRetainUntilDate`, or `ObjectLockLegalHoldStatus:"ON"`. It must be verified that a role without bypass really cannot delete during the retention period. The script rejects BypassGovernanceRetention=true. A legal hold is removed with `LegalHold:{Status:"OFF"}` of `put_object_legal_hold`, and it applies only to this case's version. Compliance waits for real expiry. Do not change the host clock, and do not delete files internally.

A KMS/tier outage uses NETWORK to block only the corresponding dependency's IP and port inside this case's dedicated net namespace, or revokes only this case's tenant credentials. After the original rules and permissions are restored, retry the original version. One missing remote version on a tier may be deleted through this case's remote bucket API, with the ACK and the original backup retained, but that is a "remote object lost" negative case and is not the same as a network interruption.

## 4. Admin triggers, status, and windows

<a id="CONFIG"></a>

The actual service configuration must be captured as evidence, but **do not dump the full environment when it contains credentials**. Manual mode records the following non-sensitive fields:

```text
RUSTFS_HEAL_ENABLED=true
RUSTFS_SCANNER_ENABLED=false
RUSTFS_HEAL_AUTO_HEAL_ENABLE=false
RUSTFS_HEAL_MRF_ENABLE=false
RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE=false
```

Scanner, automatic, and MRF scenarios enable the corresponding fields item by item according to the procedure card, and freeze the scanner cycle, start delay, and bitrot cycle. A configuration change modifies only the dedicated service.env. After this case's unit is restarted, prove that the change actually took effect. Queue dependencies required by read-repair are verified against the candidate source. The path under test must not be turned off in order to "exclude the background".

<a id="START"></a>
<a id="STATUS"></a>
<a id="STOP"></a>
<a id="WAIT"></a>
<a id="ADMIN_RAW"></a>

### Raw Admin commands

`SCOPE_PATH` is the encoded `/rustfs/admin/v3/heal/<bucket>/<key-or-prefix>`. Every byte of the key is URL-encoded, and root is `/rustfs/admin/v3/heal/`. pool and set in the JSON are the actual zero-based indexes, and they must not be fixed at 0 on H5. Example:

```bash
hp start admin --path "$SCOPE_PATH" \
  --body '{"recursive":false,"dryRun":false,"remove":false,"recreate":false,"scanMode":2,"updateParity":false,"nolock":false,"readRepair":false,"pool":0,"set":0}'
TOKEN=$(jq -er '.observation.response.clientToken | select(type=="string" and length>0)' "$HEAL_REPORT/start.json")
hp status-001 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN"
hp stop admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query forceStop=true
hp force-start admin --path "$SCOPE_PATH" --query forceStart=true --body-file heal-options.json
# Keep the duplicate query and actually send it to the server for verification:
hp invalid-duplicate admin --path "$SCOPE_PATH" --query clientToken=a \
  --query clientToken=b --expect-http 400
```

WAIT uses a bounded loop to save every response, not only the last green status. 900 seconds for an ordinary repair and 1800 seconds after a restart are starting test budgets. They must be pre-registered, and they are not a product SLO:

```bash
for i in $(seq 1 900); do
  label=$(printf 'wait-%04d' "$i")
  hp "$label" admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN"
  summary=$(jq -er '.observation.response.summary' "$HEAL_REPORT/$label.json")
  if [ "$summary" != running ]; then break; fi
  sleep 1
done
```

After the loop exits, a person or the candidate adapter checks: execution completed and coverage complete; failed/unknown/attemptFailures=0; no overflow; per-object disposition matches the design; and, for a non-dry-run, incarnation, version, pool, and set are correct. A dry-run allows only observed, with healed=0 and no subsequent physical change. A negative case is judged by the pre-registered error or incomplete contract. The positive completed outcome is not applied to it.

`summary=finished` or HTTP 200 alone is not sufficient. `truncated=false` may be omitted, and null or true does not count as false. status.settings is the query default and does not represent the original task's actual scanMode. The 128-item window of an outcome is not a complete object inventory. The current rc simplifies some fields. Raw Admin plus an independent manifest is authoritative.

<a id="BACKGROUND"></a>
<a id="REPLACEMENT"></a>
<a id="RC_STATUS"></a>

```bash
hp background admin --path /rustfs/admin/v3/background-heal/status
hp replacement admin --method GET --path /rustfs/admin/v4/heal/replacement-recovery
"$RC_BIN" --json --no-color admin heal status "$RC_ALIAS"
```

The rc must be this batch's frozen binary. The private RC_CONFIG_DIR and alias point only at this case's service, and stdout, stderr, and exitCode are all retained. To query another node, run heal_probe's global `--endpoint n1` or `--endpoint stable` directly, and save the identity of the peer that was actually connected. A healthy status requires expected, responded, and unknown, together with coverage of every peer. An idle false green when a peer is missing is not allowed.

<a id="WINDOW"></a>
<a id="CURSOR"></a>

### Partial progress and incremental cursors

```bash
hp cursor-001 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query sinceSeq=0
NEXT=$(jq -er '.observation.response.nextSeq - 1' "$HEAL_REPORT/cursor-001.json")
hp cursor-002 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query "sinceSeq=$NEXT"
```

The evidence required for WINDOW is that the task is running and `0<processed<pre-frozen object total>`, combined with a snapshot of the target shards proving that files have indeed not yet been created. Save the observation response, the trigger command, and the target PID and generation. When processed alone is insufficient, an explicit target completed/expected set is required. Status and sampling latency must be shorter than the fault window. If it exceeds that window, judge NOT_OBSERVED. Do not unconditionally kill the process after a fixed sleep, and do not rerun without bound until a hit happens to occur.

CURSOR saves nextSeq, minSeq, truncated, and items for every response. A gap from a cursor that is too old is an expected observation and must not be silently discarded. After cross-node operation and after a restart, group by task and epoch, and supplement evidence outside the bounded window with the complete manifest. DX 257 is the starting data volume. If it is still too fast, the next batch pre-registers additional load or a restricted fault layer, instead of temporarily changing the denominator in this batch.

For a fixed candidate, `sinceSeq` is an exclusive cursor and `nextSeq` is the next sequence number to be assigned, so after a complete response pass `nextSeq - 1`. The example above applies only when this response's items were returned in full. If the payload itself trimmed the items, advance only to `max(sinceSeq + 1, minSeq) + actual number of items received - 1`, and continue reading the suffix that was not returned. Do not hide response trimming, or a retained gap, inside a nextSeq jump. The J03 automatic executor pre-registers 1025 external objects of 1MiB+73 bytes, which actually exceeds that candidate's item retention of 1024 items. The 128-item outcome diagnostic window is verified separately. Each node's physical inventory is explicitly limited to 16384 files, and each batch's independent streaming verification covers at most 128 parts. When two valid running incremental windows are not observed, record NOT_OBSERVED, and the complete case cannot PASS. H1's cross-node branch is explicitly inapplicable. H2 queries all four actual nodes.

<a id="IAM"></a>

### Permissions and expired signatures

On the dedicated service, first save the current rc's `admin user --help`, `admin policy --help`, and `admin heal --help`. Create a minimal policy that uses the Admin Heal action supported by the candidate IAM, exported from the actual string of the candidate `HealAdminAction` without guessing the spelling, and separately create a role that explicitly does not contain that action. Example user and policy actions are executed in the parameter order given by this batch's rc help, and the raw receipts are saved. Credentials use a private input channel, and shell trace is not enabled.

The reference candidate action that has been checked is `admin:Heal`, and the minimal policy body is `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:Heal"],"Resource":["arn:aws:s3:::*"]}]}`. The RustFS policy parser requires a typed ARN, and a bare `"*"` is rejected. Grant this permission only to this case's temporary identity. Do not change the shared administrator policy. A new candidate still needs its action and the rc attach syntax checked.

```bash
hp anonymous-start admin --path "$SCOPE_PATH" --auth anonymous --expect-http 403 --body-file heal-options.json
hp bad-signature admin --path "$SCOPE_PATH" --auth bad-signature --expect-http 403 --body-file heal-options.json
# No-permission or minimal-permission instance: replace HEAL_ACCESS_KEY and HEAL_SECRET_KEY in this invocation's environment, and execute the same request.
```

Expired signatures: save one signed request and, outside the signature validity period, replay it to the **same address, path, and request body of this case**. Only wait for the client signature to expire. Do not change the server clock. First run the same request with a new signature as a control. The time limit is pre-registered from the candidate's signature-verification window. The signature itself contains sensitive identity. Store the original request in a private mode-0600 file, and archive only the redacted result and the request digest. Test start, status, and stop. A token cannot stand in for a permission. At the end, delete this case's user and policy through the normal IAM API and verify that the old credentials are no longer valid.

## 5. Physical evidence and fault backends

<a id="FORMAT"></a>
<a id="PHYSICAL"></a>

### Real layout, roles, and physical snapshots

On the target node, read this case's `data/.rustfs.sys/format.json` (read-only) and the mount UUID and major:minor on each disk, and save the pool/set/this mapping. Multiple directories on the same device must not be treated as independent disks. Per-object xl.meta uses a read-only parser built from the same source as the candidate:

```bash
python scripts/validation_heal/prepare_heal_meta_inspector.py --source "$CANDIDATE_SOURCE" \
  --commit "$CANDIDATE_COMMIT" --out "$INSPECTOR_BUILD"
# Approved build machine only; after the independent helper's lockfile is generated, freeze it separately from the service build inventory.
cargo build --release --manifest-path "$INSPECTOR_BUILD/Cargo.toml"
sha256sum "$INSPECTOR_BUILD/Cargo.lock" "$INSPECTOR_BUILD/target/release/heal-meta-inspect"
"$INSPECTOR_BUILD/target/release/heal-meta-inspect" "$XL_META_EVIDENCE_COPY" "$B" "$K"
```

The parser does not emit user metadata or keys. It emits only versions, data_dir, parts, and EC parameters and shard roles. First prove success on the healthy samples from the same batch, and refusal on the damaged samples. An unsupported version must not be declared verified for the same candidate after switching to a parser for another version. The source checkout must be a fixed SHA with no tracked modifications.

```bash
# Target node. Redirect all output to this case's evidence files. An existing stage must not be overwritten.
hf inspect --path "$TARGET_RELATIVE_PATH"
hf snapshot --bucket "$B"
```

A snapshot saves the length and hash of every file in the whole bucket. Directory descriptors reject, level by level, a symlink, a hardlink, or a different mount ID, including a bind mount of the same device. The limit is 10,000 files and 8GiB, with 64MiB per file. Each of the four stages before, fault, after, and restart has one copy per disk. An external part that is byte-for-byte identical to the **healthy original shard of the same role and the same version** is the independent rebuild control. The data should not be duplicated, and the four shards' digests should differ. Do not read quarantine to assist the server rebuild.

xl.meta may be legally rewritten. Compare the parsed versions and references, rather than requiring every internal byte to stay unchanged. An allowed xl.meta.bkp must belong to the same target data_dir, and its digest must equal the original xl.meta. That allowance must not be generalized to exempt an arbitrary extra file. Ordinary parse mode does not verify the payload. `--verify-part XL_META BUCKET KEY PART_FILE VERSION_ID PART_NUMBER` verifies HighwayHash block by block according to the actual version, part, and legacy/current framing in the candidate metadata, and it also rejects a short read, trailing bytes, and an unsupported format. The shard executor further requires, item by item, that every same-version shard exactly match the pre-fault original length and hash of the same role, and it confirms the full baseline content, the real set slot, and non-duplicate shard digests. It does not support physical acceptance of encryption, compression, inline data, or a changed encoding shape. Those cases still keep their own tool requirements.

<a id="FAULT"></a>

### Single-file faults (existing script)

First stop this case's writes and make the test processes quiescent. After the format mapping has been confirmed, select the exact physical path. Do not just find the first part. On inject and on recovery, `--service-quiescent` is the operator's confirmation. The tool will not stop the shared service for you.

```bash
hf apply --path "$TARGET_RELATIVE_PATH" --fault-id C02-data-middle \
  --kind bitflip --offset "$BYTE_OFFSET" --expected-sha256 "$BASELINE_SHA256" --service-quiescent
# Each other instance uses a new fault ID: --kind missing / truncate / zero / header
hf snapshot --bucket "$B"
```

`truncate` keeps half; `header` flips the first 32 bytes; bitflip may specify any valid offset; zero is rejected if it would not change any byte. First persist the intent and the original, then apply the change, then witness it. A repeated apply refuses overwrite. The backup is in the same run's quarantine directory, and never inside a RustFS data root.

File faults are F1, F2, and object-level F3, not whole-disk faults. The helper explicitly forbids `.rustfs.sys`, format, IAM, and any shell or device operation. When a case needs a system record or a special format, use the manual allowlist backend below.

<a id="FAULT_FORMAT"></a>
<a id="FORMAT_BAD"></a>
<a id="CHECKPOINT"></a>

### Format and system-record faults (manual allowlist backend)

First `PROCESS stop`. Locate **this case's** exact record from the candidate source or the real task, and generate `record-plan.json`: the absolute path, run/task/bucket/incarnation, disk UUID, original sha and size, the change offset or the replacement-source sha, the fields allowed to be added or deleted, the backup, and the undo. A second person, or an approved operator, reviews each item. Recursive modification of `.rustfs.sys` is forbidden.

Operable recipes:

1. **near-tail / length mismatch**: copy the target part into this case's quarantine and verify the sha. On that target copy, run `truncate -s "$NEW_SIZE" "$TARGET"`, where NEW_SIZE is the original size−1 or a specified length. Check the new size and hash, and check that the original metadata is unchanged. Execute this while the service is stopped. The target path comes from the reviewed inventory.
2. **Foreign format/set**: a second isolated small cluster of the same run starts normally and produces a format, and then both services are stopped. Install only that format onto this case's empty replacement root. Do not change a healthy disk. Record the original and foreign ids, this, and sets. After startup, expect a safe refusal. The original foreign format must not be rewritten.
3. **Future format version**: on the format JSON copy in this case's replacement root, use `jq` to change only the verified version field to an unsupported value. Archive the original and the change diff. A future object or checkpoint schema changes the corresponding version header according to the candidate's codec structure. Do not flip bits at random and then call the result a "future schema". If a checksum exists, it must be recomputed and then used to prove that the structure is parseable and that the schema is unknown; otherwise count only the bad-checksum instance.
4. **Conflicting generations/quorum**: serially perform valid writes of V1 and V2, or of two incarnations, and save a complete set of this case's copies for each. After the service is stopped, install each set that is short of R according to the pre-registered disk assignment table. Before restart, check disk by disk with FORMAT and prove that no same-version quorum exists.
5. **Bad checkpoint/MRF records**: read-only save `.rustfs.sys/root-heal-<task-id>.json` and the candidate's actual journal path, and confirm that they belong to this run. Truncate them, replace them with a stale generation retained from an early stage of this run, or change only the checksum field. Do not delete an entire queue on the basis of a directory name. A write failure is constructed through IO_FAULT. Do not change permissions and then call it a failure without a witness.
6. **bucket metadata**: include only the records that correspond to this case's bucket. Establish a healthy backup of the same run, then restart and heal after a missing case and after a truncated case respectively. Check the original policy, tags, and version and lock configuration through S3. The digests of the other system files stay unchanged.
7. **inline offset**: use a codec that matches the candidate to locate the target version's inline map and value in the evidence copy. Save the parsed offset and length and the reverse mapping, and then modify only that value. If it cannot be parsed, do not arbitrarily flip the xl.meta header and present that as inline damage. Use the already defined "bad metadata" instance instead. The original inline instance remains uncovered.
8. **orphan/dangling**: form historical directories through normal completion, overwrite, and deletion. After FORMAT proves every live version and MPU reference, test remove. The grace period waits according to real time and this case's service configuration. Do not change the host clock. The shared-reference control must be retained.

Every step of the manual operations above requires an exact backup of the original, hashes before and after the change, the operation's return value, and a recheck. Do not `rm` a record directly. Moving with `mv -n`, on the same disk, to a quarantine path can retain the original, but the disappearance of the source and the identical target digest must be verified, because an `mv -n` skip does not necessarily report an error. On recovery, first retain the new records produced after the failure. If there is a new ACK or generation, copying the old metadata back is forbidden.

<a id="DRIVE_GATE"></a>

### Logical disk unreachable, read-only, and permissions

Use this only inside **this case's service mount namespace** where PrivateMounts is already set. First prove that the target PID still belongs to this case's unit, that `/proc/$PID/ns/mnt` differs from the host, and that the target mount is not shared. Do not enter the host namespace to unmount `/data/rustfsN`. For the principle, see [Linux nsenter](https://www.man7.org/linux/man-pages/man1/nsenter.1.html).

Read-only recipe (on the target node; DATA is this case's complete data root):

```bash
sudo nsenter -t "$HEAL_PID" -m -- mount --bind "$DATA" "$DATA"
sudo nsenter -t "$HEAL_PID" -m -- mount -o remount,bind,ro "$DATA"
sudo nsenter -t "$HEAL_PID" -m -- findmnt -T "$DATA" -o TARGET,SOURCE,OPTIONS
# Using the service's actual UID, write-probe a temporary file of this case in that namespace; EROFS must be observed.
# Remove only this extra bind layer:
sudo nsenter -t "$HEAL_PID" -m -- umount "$DATA"
```

For unreachable, use an approved fault layer in that service's mount namespace to cover this data root (a dedicated device-mapper device that returns EIO; see IO_FAULT). Do not treat covering the root with an empty directory as a lost disk, because that may trigger new-disk initialization. A permission instance may use this case's non-root UID plus an exact directory ACL denial, and EACCES is measured under the same UID. A bare chmod against a root service does not count as a valid injection. After recovery, re-verify the mount and disk identity and that read and write are reachable. The helper's cross-mount refusal inside the namespace is correct protection, and it should not be turned off.

<a id="REPLACE"></a>

### New logical-disk replacement

Stop this case's unit and its writes, and check ROOT/data against the owner, the run, and the mount. Move the **exact data root**, on the same disk, into the unique old-disk directory of this case's quarantine, and create a new empty data root in the same location. Leave the other disks untouched. After restart, read format and confirm the new disk UUID and slot. If format initialization fails, record it as it happened. Do not manually supply a UUID.

```bash
# Only this case's reviewed ROOT; first verify that the backup target does not exist. Do not recursively delete any directory.
sudo mv -n -- "$ROOT/data" "$ROOT/quarantine/$FAULT_ID-old-disk"
sudo install -d -m 0700 -- "$ROOT/data"
```

Check that the old root has really been moved away, that the digest inventory is complete, and that the service cannot access quarantine; then start normally. During rebuild, do not copy parts from old-disk. For a failure rescue, first stop the unit and move the new failed data root to another retained directory, then restore the old root. That recovery is explicitly a rescue, not a heal result. Real device detach and attach belong only to a separately approved H6, and they are not simulated by this command.

<a id="PROCESS"></a>
<a id="UPGRADE"></a>

### Process stop, crash, restart, and upgrade

```bash
sudo systemctl show "$HEAL_UNIT" -p MainPID -p ExecMainStartTimestampMonotonic -p FragmentPath -p ActiveState
sudo systemctl stop "$HEAL_UNIT"
sudo systemctl start "$HEAL_UNIT"
# Independent crash instance; WINDOW must come first, and the actual MainPID that is killed must be recorded:
sudo systemctl kill --kill-whom=main --signal=SIGKILL "$HEAL_UNIT"
sudo systemctl show "$HEAL_UNIT" -p ExecMainCode -p ExecMainStatus -p Result
sudo systemctl reset-failed "$HEAL_UNIT"
sudo systemctl start "$HEAL_UNIT"
```

Before every operation, check the MainPID, the exe digest, and starttime. Refuse 0 or a PID that has already been replaced. Do not `pkill rustfs`. Save `/proc/sys/kernel/random/boot_id`. The boot_id is the same before and after SIGKILL, so this must not be called a power loss.

UPGRADE stops this case's nodes one by one and replaces that unit's exec path with a second, already verified, optimized release binary, retaining the old binary. The original env, endpoint, and data stay unchanged. After start, check each peer's actual version. Both the old and the new build inventories, the package shas, and the library-format compatibility table must be pre-registered. An unknown format forbids a direct rollback; test safe refusal instead. Responsibility during the exact upgrade is verified according to CHECKPOINT. Compatibility is not judged from a version string.

<a id="NETWORK"></a>

### Test-port partition / dependency fault

Prefer a pre-allocated dedicated net namespace, and prove that it differs from the host. Routes required for business-node connectivity are already configured, and SSH still goes through the host. Add rules only for this case's API/RPC port, or for this case's KMS/tier endpoint. First save the rules and routes, and set an independent timeout-recovery task. Example:

```bash
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -I OUTPUT 1 \
  -d "$TEST_PEER_IP" -p tcp --dport "$TEST_SERVICE_PORT" \
  -m comment --comment "$FAULT_ID" -j DROP
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -nvL OUTPUT --line-numbers
# In the same namespace, delete the fully identical rule. Do not flush any chain:
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -D OUTPUT \
  -d "$TEST_PEER_IP" -p tcp --dport "$TEST_SERVICE_PORT" \
  -m comment --comment "$FAULT_ID" -j DROP
```

A bidirectional partition is applied in the namespaces of both bound parties, and it must not be expanded to port 22, the management plane, or host OUTPUT. Record the drop counters and the probes at both ends. When there is no independent net namespace, or no approval for a dedicated host rule, that instance is BLOCKED. Do not disconnect the network globally in order to force a result.

<a id="IO_FAULT"></a>
<a id="QUOTA"></a>
<a id="LOCK_WINDOW"></a>

### Slow IO, EIO, capacity, and the lock window

This group operates **only on a pre-allocated exclusive fault disk, or on a dedicated loop device with a hard limit**, and it never runs against a shared production device such as `/dev/nvme*`. Before operating, record the backing file, the loop or device-mapper name, major:minor, the mount UUID, the limit, and the unique FAULT_ID. Creating or formatting a device requires a separate resource approval for this batch. It is not a default authorization of the case.

device-mapper recipe for an already allocated dedicated `HEAL_DM_NAME` / `HEAL_BACKING_DEV`:

```bash
sudo dmsetup table "$HEAL_DM_NAME"
sudo dmsetup suspend "$HEAL_DM_NAME"
sudo dmsetup reload "$HEAL_DM_NAME" --table "0 $HEAL_SECTORS delay $HEAL_BACKING_DEV 0 200"
sudo dmsetup resume "$HEAL_DM_NAME"
# The EIO instance uses a table that returns error entirely; use it only for the dedicated fault layer:
sudo dmsetup suspend "$HEAL_DM_NAME"
sudo dmsetup reload "$HEAL_DM_NAME" --table "0 $HEAL_SECTORS error"
sudo dmsetup resume "$HEAL_DM_NAME"
```

Recovery reloads the exact original table that was saved beforehand, and does not guess a linear offset. In the same service namespace, witness fixed-length reads and writes, recording the elapsed time and errno, and verify that the source device is usable. Block-device EIO is not the same as an application-layer short read. A short-read instance uses FAULT truncate together with real read-error evidence. If a "short read with no content change" is required, an allocated faultfs backend must exist, and that capability condition is recorded separately.

ENOSPC writes placeholder files only into this case's **hard-limit volume**. First use `findmnt`, `df -B1`, and the inode count to confirm that the outer XFS will not be exhausted. Fill in chunks up to the limit until ENOSPC is returned, and record the actual service write failure. An inode instance exhausts that volume's inode budget by creating empty files. Do not create an unlimited number of files on a shared root. Release deletes only this case's enumerated placeholder files, or reclaims the dedicated volume. Do not use a broad recursive directory deletion.

Bucket quota uses the frozen rc's `quota --help`, and runs `quota set` and `quota info` on this case's bucket. Queries before and after prove that the limit really took effect. At the end, restore the original configuration. Do not treat client-local rate limiting as a server quota.

```bash
"$RC_BIN" --json --no-color quota info "$RC_ALIAS/$B"
"$RC_BIN" --json --no-color quota set "$RC_ALIAS/$B" 64M
# Clear only when the original configuration had no quota and this case is ending; otherwise restore the original limit:
"$RC_BIN" --json --no-color quota clear "$RC_ALIAS/$B"
```

LOCK_WINDOW: the slow-IO layer acts on one request that actually holds the object write lock, and a same-key heal or PUT is issued separately. Use logs, or a native runtime fence, to prove the order of lock acquisition, timeout, caller exit, and in-flight commit exit. If there is no observable lock window, the black-box instance is NOT_OBSERVED. A client disconnect may end **this request process** with a bounded `timeout`, without killing the service. Afterward, continue collecting the server-side in-flight state. Exact ownership proof depends on NATIVE. Do not infer from a single timeout that the lock has been released.

<a id="HOST_POWER"></a>

### Forced stop

Execute only against the exact Azure resource ID already approved for H6. First verify that the VM is exclusive, that no other service is present, the disk persistence policy, and out-of-band recovery. Record the current boot_id and the platform status:

```bash
az vm get-instance-view --ids "$APPROVED_EXCLUSIVE_VM_ID"
az vm stop --ids "$APPROVED_EXCLUSIVE_VM_ID" --skip-shutdown
az vm start --ids "$APPROVED_EXCLUSIVE_VM_ID"
az monitor activity-log list --resource-id "$APPROVED_EXCLUSIVE_VM_ID" \
  --start-time "$RUN_STARTED_AT" --end-time "$RUN_FINISHED_AT"
```

This is a real forced stop of the VM. It does not prove that a physical controller lost power. A physical-host power cycle must use the out-of-band console already approved for that exclusive machine, and it must record the BMC events and the power cycle. Do not invent a generic command for an unknown machine model. Without authorization, do not execute it, and do not substitute SIGKILL for a pass.

## 6. Concurrency, Pool, ILM, replication, and native evidence

<a id="WORKLOAD"></a>
<a id="METRICS"></a>
<a id="LOGS"></a>
<a id="PERFORMANCE"></a>

### Bounded load and observation

`trace.json` is an ordered array of S3 operations, for example:

```json
[
  {"operation":"put_object","params":{"Bucket":"rv-heal-20260910-a01","Key":"race-${worker}"},"size":5243003,"expectHttp":200},
  {"operation":"head_object","params":{"Bucket":"rv-heal-20260910-a01","Key":"race-${worker}"},"expectHttp":200}
]
```

```bash
hw writer run --trace trace.json --rounds 100 --workers 2 --interval 0.1 --duration 300
```

The same worker is serial. Different workers that use different keys can make the order explicit. When testing a real same-key race, a person builds the expectation from the ACKs and from uncertain results. `${round}` and `${worker}` are only JSON string substitution, not eval. Upload content is determined by run, worker, round, and step, and the size and digest are written into the ACK. Each ACK is immediately written to its own exclusive file. An unexpected error stops further admission and retains the in-flight results. Do not retry without bound because the service returned an error.

A long-stability branch raises the approved `--duration` to 86400 or 259200, with enough rounds and rate limiting, and it checks the actual elapsed time. Exhausting the rounds or the byte budget early does not count as having filled the window. Each batch has a write budget of at most 1,000,000 requests, 16 workers, and 64GiB. It may run as several pre-registered segments, and a continuous timeline must be retained. Scattered short runs must not be spliced together afterward and presented as a continuous 24h.

Admin queue pressure calls hp with the same bounded concurrency, for example at most four fixed tasks in the shell background. First freeze the request set, and save each return under an independent label. An S3 load must not be treated as coverage of Admin admission.

```bash
# Sample on the target node at a fixed rate, with a time window that matches this case; do not collect credentials from the full environment.
sudo journalctl -u "$HEAL_UNIT" --since "$RUN_STARTED_AT" --until "$RUN_FINISHED_AT" --no-pager
sudo systemctl show "$HEAL_UNIT" -p MemoryCurrent -p TasksCurrent -p CPUUsageNSec
pidstat -dru -p "$HEAL_PID" 1 60
iostat -dx 1 60
```

Also record the FD count, the target IO, the queue and persisted records, and temporary space. If a tool is not installed, preflight first blocks the performance evidence that is required. Do not finish the run and only then omit that evidence. Performance ABBA uses four rounds per group and at least three groups. Warmup, concurrency, content, the cache and the initial queue, the load window, and the thresholds are fixed. Separately compute GET and PUT P95 and P99, the error rate (every failure and timeout is in the total denominator), repair throughput, and CPU and IO. Without a pre-registered threshold, only a measurement report can be produced. A performance pass must not be recorded.

<a id="POOL"></a>

### Pool interaction

Use the rc alias of **this case's H5 service**. The existing 13 Pool regression results are only a precondition control. Do not directly invoke the deploy script that would reset the old fleet. Scale-out and removal topologies reuse the reviewed endpoint-grouping plan, but the root directory is replaced by this case's heal namespace. Every poolId is taken from the actual status and from the format mapping.

```bash
"$RC_BIN" --json --no-color admin rebalance start "$RC_ALIAS"
"$RC_BIN" --json --no-color admin rebalance status "$RC_ALIAS"
"$RC_BIN" --json --no-color admin rebalance stop "$RC_ALIAS"
"$RC_BIN" --json --no-color admin decommission start "$RC_ALIAS" "$POOL_ID" --by-id
"$RC_BIN" --json --no-color admin decommission status "$RC_ALIAS"
"$RC_BIN" --json --no-color admin decommission cancel "$RC_ALIAS" "$POOL_ID" --by-id
"$RC_BIN" --json --no-color admin decommission clear "$RC_ALIAS" "$POOL_ID" --by-id
```

Do not run every operation above through to completion in sequence. Execute them in the corresponding window according to the H01–H06 procedure cards. Each time, save the rc stdout, stderr, and exitCode, the related Admin status, and the manifests before and after. Decommission write restrictions follow the established product contract. When only the survivor is verified, the source must actually be stopped and its configuration removed. Whether the critical points of #2422 and #2423 actually fire is registered separately.

<a id="ILM"></a>
<a id="REPLICATION"></a>

### ILM, remote tier, and replication

Use the lane already implemented in this repository as a **control experiment** to check the candidate's functionality, and re-establish a dedicated bucket and remote tenant on this case's independent service. The configuration JSON contains only this case's resources. The API operations follow:

```bash
hp ilm-put s3 --operation put_bucket_lifecycle_configuration --params-file lifecycle.json
hp ilm-get s3 --operation get_bucket_lifecycle_configuration --params-file bucket.json
hp restore s3 --operation restore_object --params-file restore-version.json --expect-http 202
hp replication-put s3 --operation put_bucket_replication --params-file replication.json
hp replication-get s3 --operation get_bucket_replication --params-file bucket.json
```

`lifecycle.json` uses the real durations of the candidate that has been verified. The rule ID contains the run, Filter is only this case's prefix, and Expiration, NoncurrentVersionExpiration, and Transition are configured respectively. Do not use an unmerged minute-level test hook and present it as a release. Remote-tier initialization follows the actual invocation of the current rc `ilm tier add --help` and of the existing `scripts/validation_ilm/ilm_behavior.py`, filling in this case's endpoint, bucket, and region and the controlled credentials. Raw credentials are not archived.

The replication configuration contains this case's role and destination ARN, Filter, and DeleteMarkerReplication. First prove the peer's version mapping and deletion semantics. resync uses the actual command supported by this batch's rc `replicate resync --help`, and saves the start ID and the status at both ends. Observe the real overlap window. Without a tier or a second endpoint, the related branch is BLOCKED rather than that item being deleted. At the end, delete this case's lifecycle and replication configuration through the API, and clean this case's remote data according to the version inventory. Do not change the rules of a shared service.

Resync operations of the reference rc that have been checked (a new batch must still verify them against help):

```bash
"$RC_BIN" --json --no-color replicate resync start "$RC_ALIAS/$B" \
  --target-arn "$TEST_TARGET_ARN" --reset-id "$RUN" --yes
"$RC_BIN" --json --no-color replicate resync status "$RC_ALIAS/$B" --target-arn "$TEST_TARGET_ARN"
```

<a id="NATIVE"></a>

### Exact barriers and supplemental native-regression evidence

An F9-class window cannot rely on an ordinary release black box to guarantee a hit. Use an **independent native regression binary** built from the same source SHA, and never replace the optimized release service under test. First list the real tests and freeze the complete test name, the crate, the source location, and the expected barrier events:

```bash
cargo test -p rustfs-heal -- --list
cargo test -p rustfs-ecstore -- --list
cargo test -p e2e_test -- --list
# NATIVE_TEST must come verbatim from the list above, and the log must show exactly one item and a barrier hit:
cargo test -p "$NATIVE_CRATE" "$NATIVE_TEST" -- --exact --nocapture
```

Existing replacement regression functions include `test_cluster_root_heal_recovers_remote_shards_after_background_target_restart`, `..._crash`, and the EC8+4 and multi-set versions. The fully qualified names are taken from that commit's --list. By default they create local E2E temporary directories, and they are **not a remote deployer of real independent disks**. Do not point these tests, which clean temporary directories, at the existing fleet through environment variables. Native lock, generation, and checkpoint tests are run only after the exact regression and its barrier marker have been found, in the source of the same commit, according to the selected procedure card. A branch with no corresponding barrier retains the capability gap "deterministic tests need to be added on the source side". Do not feed handwritten JSON to the plan generator and claim that the result was measured.

Archive the candidate SHA, the native-test source SHA, the complete command, the test count, the exitCode, the runtime, and the barrier events. Native and fleet are scored separately. Missing exact native evidence does not affect the truthfulness of black-box branches that have already been completed, but it cannot be announced that every branch of that case is complete.

## 7. Recovery, reporting, and safe handover

<a id="RECOVER"></a>

First save the state, the logs, and PHYSICAL from the time of the error, and stop this case's load, tasks, and processes. Then, according to the fault receipt, revoke the rules, the read-only layer, and the replacement root. A restricted recovery is allowed for a lease that is already invalid. Continued injection is not allowed. Single-file recovery:

```bash
hf recover --fault-id C02-data-middle --service-quiescent
```

Restore only a backup from the same run. Refuse the restore if the original's digest does not match. If the target is already the original correct bytes, do not rewrite it. If it has become bytes that are neither the original nor the post-injection bytes (there may be a new ACK, or a legal metadata rewrite), refuse a blind overwrite and leave the scene for a person to retain. The recovery tool outputs `physicalRepairPassAllowed=false`. Any `restoredFromBackup=true` cannot be used as evidence that the product repaired or rebuilt the shard.

For an externally cancelled Functional Chain, `validation_cleanup_run_id` accepts only a completed run on the same workflow branch. F07 recovery supports the bounded H2/DV stale-generation boundary after four missing-part injections and before Admin admission: all 18 earlier branches must have complete cleanup receipts, and all 282 fixture requests must have matching successful SDK acknowledgements. Any additional action, unknown write, lost root or physical drift fails closed. Recovery stops the owned services, restores only the four original part backups, and verifies the complete original business files and disk formats. It then retains every data root and private state directory by the existing non-overwriting, same-filesystem archive protocol, with a durable inventory bound to each node's original business-scene digest. Partial restoration and partial archive moves are retryable. `ISOLATED` releases the active namespace while preserving backups; it never rewrites the original cancelled/RUNNING result, restores old system records, or claims canonical PASS.

A forced fault sets up a second recovery channel and a timeout revocation. A local finally is not a reliable sole recovery method when the network or the process is killed. Before exit, enumerate this case's network rules, bind layers, dm tables, test units, temporary IAM, and incomplete MPUs, and revoke and recheck each one. Retain the failed data and the backup. If retention has not expired, retain the dedicated resources. Record the remaining paths and the expiry schedule. Anything not yet recovered is DIRTY or QUARANTINED, and the next case does not proceed.

Every **actual parameter instance** is filled in with a manual conclusion and an evidence index, not only one overall green result for the base case:

```text
preflight/  infrastructure/package/rc/harness/lease/topology/pre-registered parameters
baseline/   S3 version inventory, physical baseline, ACKs, bucket configuration
fault/      exact plan, intent, original digest, fault-effectiveness witness
trigger/    raw Admin/GET/background source and the active window
control/    token/epoch/coverage/counter/negative errors/raw cursor responses
physical/   after/restart shards, metadata mapping, independent rebuild control
s3/         every version to retain or delete, ACK resolution, complete content and configuration
recovery/   fault removal, service stop, retained paths, and safe handover
logs/       bounded logs/resources/platform or native events from every related node
```

The manual conclusion is MANUAL_PASS, FAIL, INFRA_ERROR, BLOCKED, or NOT_OBSERVED, and it lists the reviewer, each assertion, and its citation. `evidence.json` contains runId, instanceId, and an array for each of the nine evidence classes. Each citation is a `path` relative to that instance's directory and a `sha256`. The base case can be reviewed only after every branch is complete. Missing resources, a window that was not hit, and incomplete evidence are not a PASS.

```bash
python scripts/validation_heal/matrix.py audit --plan reports/validation-heal-84-plan.json \
  --evidence-dir reports/heal-84 --out reports/heal-84-evidence-audit.json
```

audit checks completeness, paths, and digests. An output of `EVIDENCE_COMPLETE_REVIEW_REQUIRED` is still not a product pass report. The original failed batch is retained permanently, and a new batch after the fix is recorded separately. This document provides an operable plan for all 84 items, but the availability of the actual machines, the dedicated dependencies, and the native barriers must be verified separately. "Plan ready" must not be rewritten as "environment ready" or "already measured live".


Automatic positive fixtures are fixed to the independent shard-commitment configuration. `integrity-fleet-attestation` records this case's endpoint, node, and candidate digests, reserve-checks the candidate on each node before startup, and reads the two switches back from the actual process environment after startup. This configuration applies only to run-owned services. Failed batches of the default legacy configuration are retained separately. Protected mode does not represent mixed deployment with an old version, or default-environment capability. D07's conditional requests freeze the expectation from the server HEAD LastModified and the baseline ETag, and they do not change the host clock. 304, 412, and 416 must not contain a successful payload or an unknown transfer result.


J06's automatic entry point is `heal.performance-abba`. Before deployment, the executor freezes three ABBA groups, 30 seconds per phase, a concurrency of 2, 4 requests per second, a half-and-half split of GET and PUT, and the complete thresholds. Each phase restarts the same service and warms it up a fixed number of times. The real partial-repair window of each phase B must enclose both GET and PUT; otherwise the whole case cannot PASS. Phase A continuously checks that idle is complete and that the physical state is unchanged. Every scheduled request, including failures, unknowns, and requests that missed admission, is counted in the denominator. GET and PUT report P95 and P99 independently. The paired differences of the three groups, the actual shard-repair throughput, and the service CPU, IO, RSS, and FD are each retained as evidence. Thresholds are kept in `PROTOCOL` of `scripts/validation_heal/performance.py`. Changing a threshold must produce a new harness commit and a new verification record. This entry point does not shorten J05's 24h/72h requirement.


A02's automatic entry point is `heal.admin-scope-isolation`. H2's dedicated `scope-isolation` runtime uses the current four nodes and sixteen disks, interleaves nodes in disk order, and sets `RUSTFS_ERASURE_SET_DRIVE_COUNT=8`, requiring two real sets that each span all four nodes. The ordinary executor still accepts only the original single set. After the complete ACKs and the actual physical set inventories of the two buckets are frozen, root, bucket, prefix, object, and pool-set are each deployed anew, and the selected fault and the control fault are injected. Complete physical and streaming proof must precede GET and recovery of the control backup. H1 retains the original four-disk single set, and it explicitly records the applicability boundary that no unselected set exists. Directories on the same disk must not be presented as a second set. On this fixed candidate, the raw receipt of an unrestricted selector is allowed to be null, and the actual physical mapping is saved separately. An explicit pool or set must match the original fields exactly.

### I04 durable responsibility during queue pressure

The live `queue-pressure` permission fault targets only `<owned-prefix>-durable` on the first owned drive. The other four queue buckets and `.rustfs.sys` retain their original permissions. The live arm derives this fixed suffix from the root-owned reservation; callers cannot supply another bucket or path. Flapping keeps its separate whole-drive fault and F07 keeps its original dedicated bucket scope.

Before the three degraded-write ACKs, the same original service/mount-namespace identity must observe business read/write `EACCES` and successful system metadata read/write controls with DAC bypass removed. A whole-drive refusal or unavailable system control is rejected before those writes. Full, Merged, Dropped, all three committed MRF responsibilities, exact source-task retirement, complete six-bucket snapshots and original `769/129/129/129` fixtures remain required. Blocking the MRF journal cannot isolate queue-Full retention. A corrected fault does not establish canonical PASS until a separate complete live run is independently audited; original failed results remain retained.

Complete the owned permission probes before admitting the finite pressure workload, so SSH setup does not consume its queue window. The later actual MRF Full event after the SDK ACKs remains mandatory; scheduler pauses, deadline increases and a prior Admin Full response cannot replace that event. Terminal object identities use the existing source-reviewed mapping between Admin's all-zero UUID and S3's `null` version. Real version UUIDs, incarnation/set boundaries, complete counters and independent physical/S3 proofs remain exact; untyped version identities are rejected.

Owned Heal services request a per-unit journal budget of 1,000,000 messages per second, then read back both effective properties and record the systemd version before source attribution. Missing, zero or different settings are rejected; merely passing launch arguments does not establish effective configuration. Host-wide journal settings remain unchanged. Cursor continuity alone does not establish complete source attribution: the reader retains journald's original dropped-message notice and marks that page incomplete, including notices identified by the stable journal message ID. Source observers reject the page before accepting any task events. Existing page/event limits, resource ceilings, workload counts and deadlines remain enforced; absent source receipts in a rate-limited original run cannot establish either successful repair or a product responsibility-loss diagnosis.

### Exact-source native race subsets

`race_native.py` builds and runs the reviewed tests in `race_native_contracts.json` on a hosted tools runner. The candidate checkout must match the exact commit, tree, lock and reviewed Rust files; the producer and registry must match the exact auto-testing checkout. The build uses a fresh target directory and rejects compiler wrappers or injected Rust flags. Each selected regression runs in a separate process with one exact nonignored test. Original logs, executable digest and ordered barrier milestones are retained. These artifacts are separate from `native_regressions.py`; existing F07/C11 artifact contracts remain unchanged.

The first registered source is `6afdec9fe1b0b88007f2e5b445c19bf73d51d3ca`. G01 includes real Heal during committed overwrite cleanup in four disk/parity/mode scenarios and the existing stale read-repair final-rename test. The G02 subset covers the version-specific shared Heal owner and queued successor PUT. The I05 subset reuses the actual cancelled physical unlink owner and stale read-repair fences. These legacy subsets do not complete G02 or I05; I05 also needs its TCP disconnect, timeout and file-descriptor matrix.

Source `bd0671ba781d8ebf509f4f03533228dfb44496f1` adds complete G02 and G03 native groups. The versioned PUT group requires both the historical lock owner and an actual new-version commit while historical reconstruction is paused. The deletion group requires exact historical and current version deletion before releasing stale reconstruction. Both use real `4/2` and `16/4` local geometries, physical write-tail drain, exact surviving encoded shards and complete version bodies. Its G01 fixture pauses every shuffled cleanup slot and checks every new encoded shard again after cleanup and before GET. These source contracts require fresh same-source hosted artifacts; local tests or an older source's artifacts cannot supply them.

Source `8380466f3a27c3c3ef4115084178a0ab99a78f32` additionally requires the complete G04 marker/batch-delete group. In each geometry, creating a marker, removing an exact marker, deleting two exact data versions with separate item receipts and deleting an unversioned object all commit while older reconstruction is paused. The foreground physical tail drains before release. Every disk must retain the exact data/marker version set without duplicates, every surviving encoded shard and complete exact/current bodies; removed markers and data directories cannot reappear. Its 24 ordered milestones supplement the existing G01/G02/G03 groups.

`race_native_evidence.py` admits the downloaded artifact without executing it. It checks the successful hosted workflow attempt, exact candidate/driver/helper identity, selected source contract, native executable, complete unique regression set and every original log. Each ordered-barrier regression must emit exactly the complete source-registered milestone sequence. Regressions marked as in-test assertions retain that narrower witness type; assertion-only evidence cannot invent emitted barrier events. The result states `requiredNativeSubsetPassed`; whole-case, fleet and canonical PASS remain false. Missing source groups, old-source artifacts, ignored or zero-test output and partial evidence are rejected. Updating another candidate's registry entry does not invalidate an unchanged selected contract. Changing the producer or the selected contract requires a fresh tools artifact.

Runtime sources opt in to specific cases separately from the native registry. The `6af` source enables only G01; `bd0671b` enables G01/G02/G03; `8380466` additionally enables G04 with its complete, distinct native group. Source `757b751` enables A09 through `nolock.py`, G06 through `race.py`, and G05 through the dedicated `mpu_race_runner.py` controller. It does not substitute a different source into G01-G04. The workflow verifies the bundle and invokes the pinned helper's read-only runtime preflight before exporting it to canonical execution. A collector-only helper without the corresponding runtime adapter fails before fleet mutation. The executor repeats admission before package installation or service changes.

All original H1/H2 black-box branches, SDK ACKs, manifests, overlap witnesses, exact Admin settings and physical/S3 checks remain required. Native logical disk shapes do not certify distributed VM execution. The original `072b57b37559fbe626aeda8abf61617fdcbdd926` route retains its blocked native rows. A09, G05/G06 and I05 still require their own complete native groups and executor evidence. G04 also remains unaccepted until its same-source hosted evidence and every original live branch pass independent audit. No source or case gains canonical acceptance from a registry entry or local native test; a successful exact-source hosted bundle, every live branch and independent original-evidence audit are required.

The A09/G01-G04/G06 public race adapters retain the frozen fourth-physical-drive fault selection, independent of shuffled data/parity role. H2 selects that local drive on its first original host. After faults and restart, `race_io.py` applies the source-defined `65536` bytes/s write cap only to the run-owned unit on actual fault hosts. Exact process birth/candidate, all four original mount identities, systemd cgroup and `/proc` membership are checked; the actual kernel `io.max` must show the original fourth device and rate. Existing foreign limits, changed identity, wrong devices and unbound release reject. Intent is recorded before each single control request. After the foreground sequence, the cap is removed and kernel readback is verified before waiting for the same original task. This adds no sleep to SDK timing and changes no payload, overlap oracle or task timeout. An interrupted or uncertain control still uses existing owned-service stop and retained-scene rules. G05 refuses this cap and retains its source-defined unrestricted process I/O.

### A09/G06 native groups and frozen G06 public fixtures

Source `757b751482bb970bc321e92059690a7698554358` adds two collector-only groups. `default-heal-exclusive-owner` observes actual PUT and exact historical DELETE lock futures becoming pending while default Deep Heal exclusively owns a real staged external repair. Physical reconstruction precedes foreground publication; all acknowledged successor or retained-current shards and full bodies are checked in 4/2 and 16/4. Its sixteen events prove the storage fence, while Admin `nolock=true` normalization and public request timing still require separate evidence. `copy-and-metadata-final-rename` covers real streamed cross-key COPY source/destination, self metadata replacement and tags. Twenty-four events bind actual ACK and physical-tail completion before releasing shared reconstruction, exact stale refusal or unchanged-source repair, final source/target/control payloads, metadata and tags. The registry binds all five storage fixture/barrier source files. This candidate admits A09, G05 and G06 only with their distinct complete case-specific native group and matching hosted helper/source artifact. Both workflow and executor run read-only preflight before bootstrap. A09 always sends wire `nolock=true` and requires effective `nolock=false`, plus both original H1/H2 public branches and the separate storage-native proof. G05 uses the separate same-key runtime described below.

The G06 public adapter uses the existing DS workload: all 257 original keys, including Chinese, spaces, literal `%2F`, actual slashes and parent-like prefixes. The observed missing key is `中文/文件/0252.bin`; its companion is `literal%2Fslash/0254.bin`. `copy-source` copies from the observed key into the companion; `copy-destination` copies in the reverse direction. Default COPY expectations inherit the source payload, content properties, metadata and tags. Both directions verify the unchanged source, exact destination coding-index shard bytes and a distinct destination generation before GET, rejecting retained old destination directories. Registry, fixture and local transition tests do not establish public overlap or complete canonical PASS.

G06 reuses `KeepAliveStatusProbe` only while observing the original token and
foreground operation. Existing readiness polls warm the same direct connection;
the controller still starts each foreground observation from the real SDK send
notification. Every connection attempt, actual socket identity, response and
phase remains in `G06-status-transport`. A transport error poisons the session
without an implicit retry; leaving the session closes its connection. The strict
same-clock nested interval, complete ACK ledger and finished-task refusal remain
unchanged. No delay is injected into the SDK call.

Before race allocation and each explicit service restart, the runner and every
participating node resolve the existing `rustfs-node1` alias (H1) or all four
aliases (H2). `endpoint-resolution` retains bounded, single-attempt `getent`
results and refuses a failed precondition. It never edits hosts files or DNS,
substitutes IP addresses, extends the readiness budget or certifies node identity.
Later topology and physical checks remain required; successful preflight cannot
guarantee that a resolver will remain available during startup.

G01/G02 retain every serial PUT ACK but follow the frozen source's overlap rule:
at least one strictly nested running status sample per required workload role.
G01's read-repair branch additionally journals every actual GET, verifies its
complete body and joint ETag/length/type/metadata against one original or
ACK-known foreground generation, and requires a reader overlap. Zero reads,
unknown results and mixed generations cannot pass. G02 requires all six
ACK-known historical versions and a separate reader overlap in its read branch.
Sampling continues until an in-flight reader drains after the writer. These
conditions preserve the source workload; they do not supply live acceptance.

### G03 serial deletion authority

G03's frozen `VersionLedger.history` determines the new current version after an explicit DELETE. The public S3 inventory is UUID-sorted and cannot supply that order. Each original foreground plan now carries the three seed ACK version IDs in order; the final oracle rejects missing, duplicate, foreign or differently selected identities, removes exactly the acknowledged version and promotes the last retained seed ACK. The physical oracle permits only that `isLatest` change while preserving every other retained descriptor, coding-index byte and data directory. Every original drive must contain exactly the retained version/latest population, with no extra marker or duplicate version. Focused tests use nonmonotonic UUIDs, both delete branches and corrupted physical populations. This source adaptation adds no native or live acceptance.

### Frozen version and multipart race controllers

G04 and G05 use the owned `frozen-race-legacy` runtime profile. It explicitly sets `RUSTFS_SHARD_INTEGRITY_WRITE=false`, `RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED=false`, `RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE=false`, and `RUSTFS_HEAL_MAX_CONCURRENT_HEALS=1`, matching the frozen source's legacy shard population and serial scheduling. The profile retains disabled scanner, automatic Heal and MRF. The regular `manual` profile continues to enable protected shard writes. Original physical file-population assertions remain strict; an unexpected integrity sidecar is a failure. Owned service configuration is attested through the existing runtime checks. G04 alone may use the frozen fourth-drive write cap under this profile; G05 only observes kernel limits. Retention supports both historical `manual` evidence and the new profile, with the same fixed bucket/key scope and stopped original scene binding. An older helper's tools remain bound to that helper; they cannot certify a newer controller.


`version_race_fixture.py` preserves the frozen `heal_version_race_fixture.py` model at `524eb5b32c3236497fdd8a9271df0494fa3d9974` (source SHA256 `755f476ab567dc413e9798bb2bcac15252c3e8308e4b15b15a9eb3083d1df485`), with local import adaptation only. Its eight G02–G04 plans derive versions from serial ACK order. G04 uses flat keys, seeds a latest marker for both marker removal and batch deletion, and deletes exactly the oldest target version, newest target version, marker and control `objects-0002.bin` version. It preserves the middle target generation. A partially acknowledged batch retains each known effect and permanently blocks final acceptance and further mutations. Historical reads address exact versions; negative reads require the original deleted-version 404 and marker 405/current-marker 404 contracts.

`version_race_physical.py` preserves the frozen physical oracle, replacing the source transport's fixed inspector digest with a mandatory caller-supplied immutable tool digest. `version_race_adapter.py` maps every owned drive to its real format set slot; H1 retains EC2+2 and H2 explicitly requires the deployed EC12+4 geometry. Every drive must match the ACK-derived version/marker population and coding indices. Deleted generations' unreferenced bytes remain separately reported; they are neither live redundancy nor a garbage-collection PASS.

`version_race_runner.py` replaces the superseded generic G04 fixture and shard-fault path. Its 257 faults remove the first host's fourth-drive `xl.meta` files. The original 120-second partial window requires nonzero original-token progress, a physically reconstructed control, at least 32 pending objects and the selected target metadata still absent. One original SDK mutation runs with the source 90-second observation budget and 25ms sample pause; the owned fourth-drive 65536-byte/s write cap is released while the original call drains. Final checks prove the 253 other controls, exact version/marker/coding populations, full live part streams, every peer's deleted-version/marker negatives and complete inventory, post-GET physical immutability, ACK-bounded completion and original root retirement. Final assertion failures preserve independent diagnostics; evidence I/O failures stop execution. Seed or foreground uncertainty retains the stopped scene and original journals as DIRTY. Recovery can isolate that exact scene without restoration, retry, deletion or a PASS rewrite. Runtime entry requires source `8380466` and the complete marker/batch native group before bootstrap; the legacy source cannot enter this controller. These implementation/component checks add no canonical PASS; fresh exact-helper native tools and complete H1/H2 live acceptance remain required.

`mpu_race_fixture.py` migrates the frozen validation repository's `heal_mpu_race_fixture.py` model at `524eb5b32c3236497fdd8a9271df0494fa3d9974`. It uses the existing auto-testing `Recorder` and probe directly. Imports are local; no source validation checkout is needed at runtime. Part sizes follow the frozen common DM recipe: `5MiB+17`, `5MiB+93`, and `1MiB+47`.

The caller supplies a fresh versioned owned bucket. One completed three-part object and two distinct pending uploads share `objects-0256.bin`: A initially has parts 1/2, B has parts 1/2/3. The model binds each mutation to its actual UploadId/part ETags and durable request/ACK files. A continues with part 3 and completes; B aborts. Unknown or malformed mutation results poison the model and prevent later sibling changes. Bounded paginated listings, exact terminal `404/NoSuchUpload`, all acknowledged historical/current versions and complete GET payloads verify sibling isolation. The model does not clean up uploads with uncertain outcomes.

Immutable native source `09fb813a003e7964bd1b893efb7446a3c41bd3bf` adds `multipart-continue-complete-abort-final-rename`: ten ordered milestones across 4/2 and 16/4 logical geometries, using real three-part commits, same-key A/B pending parts and a paused shared Deep Heal. Completion tails drain before stale Heal release; exact retry disposition, old/sibling preservation, new committed bytes and upload retirement are asserted. The native test's unversioned logical scope is supplemental to the full versioned SDK/fleet case.

The historical `09fb813` entry remains collector-only. Runtime admission binds source `757b751482bb970bc321e92059690a7698554358`, its complete multipart native group and the exact hosted helper artifact. The old `072b57b` source is rejected before bootstrap. Component tests and native bundles do not add canonical PASS.

`mpu_race_runner.py` replaces the generic unversioned MPU path with the frozen same-key fixture and schedule for all three public branches in H1/H2. It records 256 versioned flat-key controls, one committed three-part version and A/B pending uploads through the existing durable `Recorder`. The 257 faults target the fourth physical drive on the first host. All 259 original and 262 final part cohorts receive full streaming verification. Every companion ACK has an immediate independent staging checkpoint; only the selected first SDK operation requires a contained original running-token sample. The status transport preserves the frozen warmup, 1024-sample/90-second budget and 5ms poll pause, restricts requests to the owned bucket and original token, and records request/response persistence phases. Kernel I/O sampling reads the owned cgroup and its ancestors without setting or releasing a cap; it makes no continuous no-limit claim.

Final committed/staging/control proofs precede all-peer S3 verification. Assertion failures retain the frozen source's remaining independent diagnostics while evidence I/O errors stop execution. Repeated terminal responsibility, ACK bounds, post-GET physical stability and exact original-root retirement must pass before cleanup authority exists. Any issued seed request without that authority retains the stopped raw MPU prefix, committed scene and original request/ACK files as DIRTY, with no retry, abort, restore or namespace cleanup. A missing overlap remains `NOT_OBSERVED` even after safe cleanup. Full hosted native acceptance and all six public live instances still require independent evidence before G05 can count as canonical PASS.

The additive `mpu-race-scene` node action in `mpu_race_physical.py` adapts the frozen `heal_h1_mpu_physical.py` upload-ID and byte-continuity oracle to the current owned four-drive hosts. Each request binds the owned race runtime, bucket, candidate binary and all three ACK-selected uploads. It inventories the complete fixed key prefix, hashes only selected staging files under bounded entry/depth/byte limits, rejects unknown uploads and links, and preserves original process identity during a live capture. Stopped captures additionally verify the owned service remains stopped. Across H1/H2, pending part/sidecar populations and existing sibling bytes remain exact; a completed upload must lose its staging directory and retain all three previously witnessed encoded parts in committed storage. These are staging/continuity witnesses; independent inspector coding/bitrot checks, product Heal proof, real overlap and full SDK inventories remain required.

### I05 caller-only TCP disconnect component

`caller_abort.py` sends one signed, exact historical-version S3 GET to a bound endpoint and closes only that caller TCP socket. Invoke it with `python3 -B scripts/validation_heal/caller_abort.py --binding <binding.json> --bucket <owned-bucket> --key <exact-key> --version-id <acknowledged-version-uuid> --output <new-evidence-directory>`. Credentials use the existing `HEAL_ACCESS_KEY`, `HEAL_SECRET_KEY` and optional `HEAL_SESSION_TOKEN` environment variables. The request intent is persisted before connection; a completed write checkpoint precedes shutdown, and the final receipt binds the original socket addresses, descriptor, request digest and monotonic clock. The evidence directory cannot be reused. There is no request retry, redirect, proxy or response-derived success. A persistence or transport error closes the socket and retains its uncertain result. Signed headers and credential values are not stored.

The component always reports `resultUnknown=true`, `acknowledged=false` and `serverOwnershipProven=false`. Successful local command execution means the client transport completed its disconnect sequence. It does not prove that the server admitted the call, acquired a lock, continued after disconnect or released its owner correctly. I05 remains outside canonical dispatch until the controller binds actual server admission and owner events to this exact call, proves the registered timeout and stale-generation fences, and verifies final object generations, drained locks and file descriptors across every required H1/H2 branch. Existing exact-source native owner tests remain separate evidence.

### I05 service admission and caller drain observer

`lock_observer.py` observes an existing run-owned H1/H2 service using the explicit `lock-timeout` runtime profile and reviewed source `757b751482bb970bc321e92059690a7698554358`. The profile enables only the existing object-lock diagnostic target at TRACE, sets the actual object-lock acquisition budget to one second, disables early PUT rename ACKs, and preserves the standard disabled scanner/automatic-Heal/MRF settings. It does not modify the service binary. Run `python3 -B scripts/validation_heal/lock_observer.py --binding <binding.json> --nodes vm000 [vm001 vm002 vm003] --bucket <owned-bucket> --entry <acknowledged-historical-entry.json> --output <new-evidence-directory>`. The entry contains the original `key`, `versionId`, `deleteMarker=false`, and complete ACK-bound `payload.size`/`payload.sha256`. This component neither deploys nor cleans a namespace; the enclosing case owns those steps and recovery.

After persisting and sending the original single GET, the observer waits up to ten seconds for its actual server-generated request ID and `get_object` lock attempt. A unique User-Agent marker connects the exact version URI and method in the `http-request` ancestor span to that lock's owner/namespace/attempt. The marker is correlation data, not an identity asserted by the client. Its HTTP span must carry one exact historical version selector; an optional `requested_version_id` diagnostic field must agree with that selector. Free-text messages, nested native-test capture rows, client-supplied request IDs, repeated lifecycle events and foreign endpoint/owner/version records cannot prove admission. Cursor-bounded raw journald pages remain in evidence, and source message loss prevents completion.

Before disconnect, two `/proc` samples must connect the exact client/server TCP tuple to an actual established socket descriptor of the original candidate process. A network-namespace table entry alone is insufficient: its inode must also appear in that process's FD links. The read-only adapter accepts no arbitrary PID, path, command or service port. Process birth, binary, environment and mount namespace must match, and its network namespace cannot change during sampling. A second journal capture must still show acquired without a diagnostic drop. An unavailable/already-ended window records incomplete evidence and closes the original caller socket without retry.

After the caller closes, the observer follows the same request/attempt/owner until its `guard_dropped` event, requires two complete samples without the original owned socket, and waits up to thirty seconds for each peer's FD count to return to its baseline. It then reads and verifies the exact historical version's full content and samples resources again. Every transport result remains UNKNOWN. Diagnostic guard drop does not prove distributed release, snapshots do not prove ownership across the disconnect instant, and aggregate FD counts do not prove absence of every leak. Those claims stay explicitly false. The timeout/stale-generation native fences, queued-successor lock closure, complete DL/DV physical and ACK verification, every H1/H2 branch and independent live acceptance remain required; I05 stays outside canonical dispatch.

For the I05 runtime, process resource baselines and later samples also retain a bounded `descriptorCensus`: FD numbers, descriptor kinds, hashed link targets and TCP rows whose socket inodes belong to that exact process. The caller socket's two samples include the same census. Raw descriptor paths and descriptor contents are excluded. Collection rechecks the original process birth and network namespace, counts descriptors that vanished during the read, and marks every census as a snapshot without a no-leak claim. These records help diagnose a count that does not return to baseline; they do not exclude any descriptor from the original FD threshold or extend the drain budget.

### I05 object-lock timeout diagnostic controller

`lock_timeout.py` extends the caller observer with one real contender PUT while the exact historical GET still owns its read lock. Run it only against the already assigned exclusive `lock-timeout` H1/H2 service and the current auto-testing nodes: `python3 -B scripts/validation_heal/lock_timeout.py --binding <binding.json> --nodes vm000 [vm001 vm002 vm003] --bucket <owned-bucket> --entry <acknowledged-historical-entry.json> --output <new-evidence-directory>`. The original historical payload must be at least 32 MiB so the controller can apply and record a bounded 64 KiB client receive buffer, then observe the same established service-owned TCP socket with a non-empty send queue after the contender response.

The contender body, exact PUT request, endpoint, structured 503/`ServiceUnavailable`/`Retry-After: 1` response and durable ACK are bound together, including the recorded operation, parameters, payload digest and ordered timestamps in the collector's clock domain. The pinned candidate's `rustfs/src/error.rs` maps typed `StorageError::Lock(LockError::Timeout)` to this response and directly tests its header. Cursor-complete server logs must associate that response's distinct server request ID and write-attempt UUID with the same bucket/key namespace and the exact one-second `put_object_commit` timeout. The same-node GET and PUT share the set's owner label; that label is not a per-request identity. The original GET attempt must still be acquired after the timeout and socket samples. Failure, UNKNOWN, a missing journal event, an empty send queue, a small fixture, or binding drift is retained as incomplete or `NOT_OBSERVED`, never retried or passed. The controller checks the original physical scene before any S3 read, compares complete versioned inventory before and after, and waits for process FD counts to return to the original baseline.

This establishes a live timeout diagnostic subset only: it does not certify ownership at the precise disconnect edge, stale read-repair fencing, queued successor behavior, H1/H2 DL/DV source branches, or canonical acceptance. The native abort/timeout/stale fences and independent live audit remain required.

`lock_timeout_runner.py` provides the enclosing diagnostic deployment through
the existing Functional Chain workflow. After auditing prior cleanup and
ownership, explicitly select `validation_case=heal.lock-timeout-and-caller-abort`,
the exact `auto_testing_sha`, candidate build run/attempt and successful
inspector build run/attempt. The existing shared-fleet concurrency group and
auto-testing bootstrap install the immutable package. Local source, helper,
workflow identity, vm000–vm003 binding and inspector checks run before
bootstrap; Fleet verifies the installed binary digest before starting each
owned service. H1 runs on vm000, followed by H2 on vm000–vm003 only after
complete H1 diagnostic and namespace cleanup. A fresh artifact tree is required;
the runner accepts the enclosing script's setup/test logs but refuses any old
topology or summary before deployment.

Each topology uses the existing Fleet, Recorder and DV seeder: two keys have
three original versions each and one current marker; the selected historical
version is 32 MiB to maintain bounded TCP backpressure. This small fixture is a
diagnostic subset, not the complete source DL/DV matrix. After timeout and
caller-drain checks, every endpoint must preserve the entire original ACK
manifest, payloads, metadata and tags. Physical comparison brackets those
reads, and every original process's FD count must return to its baseline before
cleanup. Failed/unknown seeding, contention, observation or peer verification
stops the owned services, saves the original physical/log scene and retains
the namespace and original receipts. A failed diagnostic or cleanup prevents
the next topology. SIGTERM follows the same failure capture and retention
path. The summary records diagnostic completion while keeping whole-case and
canonical claims false. The manual entry returns canonical `BLOCKED` after a
successful diagnostic with clean finalization, preserves diagnostic failures,
and reports `INFRA_ERROR` for an incomplete summary or failed finalization
after a zero exit. The workflow gate therefore remains non-green even when
the diagnostic succeeds. I05 remains excluded from the automatic matrix;
this manual route does not certify its native barriers or complete source case.

### J02 original opaque metadata probe

The `unknown-negative` diagnostic includes the source procedure before the supplemental legacy-object diagnostic. A separate acknowledged object first receives an independently healthy Deep receipt. With the owned services stopped, the existing descriptor-relative fault backend flips byte zero of every original `xl.meta` copy: four copies in H1 and sixteen in H2. Each mutation has its own original backup, receipt and complete physical delta; the candidate-bound decoder must refuse every exact damaged copy. No other metadata, part or format may change.

A separate protected prefix is checked during the fault. The original key-level task must retain complete truthful counters and an unresolved disposition, with no invented healthy, repaired, absent, version or incarnation claim. Unknown must be counted within skipped and carry the legacy warning. If the candidate reports another truthful unresolved disposition, record `NOT_OBSERVED`; the legacy diagnostic cannot promote it to PASS. Archive the original terminal result and unchanged complete fault scene before restoring original metadata bytes. Verify the full original physical and SDK inventories and the unchanged original token after restoration. Fixture rescue is never product repair. J02 remains outside canonical dispatch until the separate version-specific grace Deferred/new-token procedure and complete live evidence are available.


### I05 retained-scene isolation

The reviewed H2 startup failure `39764496504/1` has a separate preservation
plan pinned to helper `31b787c51de23587da1f9a2ac0f910400ddf66b3` and the
SHA-256 inventory of all 102 official artifact files. H1 completed its timeout
diagnostic and actual root cleanup; H2 reserved all four peers, then failed
before fixture seeding because vm002's bpftool wrapper could not find its
kernel-specific executable. All four stopped original scenes contain no
business files and retain the same complete sixteen-disk format mapping.
The planner requires those exact original owners, candidate, inspector,
cleanup receipts and failure boundary before allowing the existing
audit/isolate/post-audit protocol. It issues no workload request and changes
neither the original FAIL nor canonical acceptance.

Isolation [39766457981/1](https://github.com/rustfs/rustfs/actions/runs/39766457981)
completed `ISOLATED` with helper `fa0b659`. Independent audit byte-matched all
102 original files and all four preflight/isolation/post-audit baselines.
All twenty data/state archive parents are root-owned mode 0700 and on their
original devices; original tree, owner and inode identities are unchanged.
No S3 request, backup restoration, namespace deletion or original verdict
rewrite occurred.

`environment.inspect` now also records the selected bpftool path, installed
`/usr/lib/linux-tools/*/bpftool` executables, resolved root-owned executable
digests, kernel release, version exit status and a read-only root-cgroup JSON
query. A working installed executable is distinct from a broken PATH wrapper.
This inventory does not install tools, attach programs, change PATH or certify
the I05 service filter; the actual owned-service attachment checks remain
mandatory before another diagnostic.

Read-only inventory
[39766570688/1](https://github.com/rustfs/rustfs/actions/runs/39766570688)
found broken PATH wrappers on both vm002 and vm003, which run kernel
`6.17.0-1022-azure`. All four peers already have
`/usr/lib/linux-tools/7.0.0-1017-azure/bpftool` with executable SHA-256
`aabc337b58169d62c42b3c8f1fccf605e5fc1e445a23feb0156638e9d9606991`.
The executable reports bpftool 7.7.0/libbpf 1.7 and its root-cgroup query exits
zero on each peer. That empty root query alone does not verify service BPF
support. I05 now uses this exact installed executable with protected root-owned
path, digest and version checks, records its identity beside the actual
service attachments, and checks every peer before reserving any namespace.
It changes no installed tool, global PATH or kernel. Service-bound ingress and
egress program identity checks remain mandatory on the running kernel.

Diagnostic [39767336741/1](https://github.com/rustfs/rustfs/actions/runs/39767336741)
completed both H1 and H2 with helper `8cbdd51` on the unchanged candidate
`757b751482bb970bc321e92059690a7698554358`, package `39364893106/1` and
inspector `39566504743/1`. Independent audit of all 211 official artifact
files verified nine seed request/ACK pairs and seven versions/markers per
topology, candidate/helper bindings, actual process-bound ingress/egress BPF
programs on all peers, complete journal cursor chains and exact server
request/lock attempt identities. Each contender returned a known non-retried
503 after a 1001 ms write-lock timeout while the original GET remained
backpressured. The caller connection disappeared and its diagnostic read
guard dropped; original physical files remained identical before public
verification and all one/four peer inventories agreed with seed ACKs.

Final FD counts were H1 vm000 `22 -> 22`; H2 vm000 `106 -> 94`, vm001
`108 -> 104`, vm002 `105 -> 101` and vm003 `101 -> 97`, with unchanged process
birth and network namespace. These are bounded snapshots, not a global
absence-of-leaks proof. All five node cleanup receipts show stopped services
and removed owned roots. The original result is `BLOCKED/cleanup PASS`:
the workflow fails its deliberately retained canonical gate despite successful
diagnostic execution. Exact disconnect-edge ownership, distributed release,
complete native timeout/stale fences, the stale-generation controller
and full source H1/H2 DL/DV coverage remain unaccepted. The separately audited
queued-successor diagnostic is recorded below. No new
canonical case is counted.

`validation_cleanup_run_id=<failed-run-id>` selects the narrow I05 preservation
route with the original package build and an immutable helper containing
`timeout_retention.py`. The driver must remain on the original Functional Chain
branch. First check the shared current auto-testing fleet for other work. The
planner accepts the original helper 704bd60 failure before contender
submission and the reviewed helper d100abeb failure after one exact known 503
contender refusal. It also accepts helper 1b88cad when H1 completed and cleaned
successfully before H2 reached that same exact contender failure. The H1 path
requires nine exact successful seed request/ACK pairs, complete
version/marker/payload/public metadata baseline, a closed read-only historical
GET, original owner/candidate/inspector and an unchanged stopped four-drive
scene. The contender boundary also verifies its request/ACK digest, server
request ID, full bucket-incarnation read-lock lifecycle and exact failed target
write-lock lifecycle. The H2 path additionally requires four reservations,
four inspector uploads, per-node lock checkpoints and balanced four-peer
journal rounds before disconnect, after admission, and during contender
observation, four stopped physical scenes and four service logs.
Unknown or successful writes, missing peers, another action, an unclean H1 or
physical drift refuse recovery.

The recovery reuses retained-scene audit/isolate/post-audit, retaining all four
roots and state under private per-run parents without S3 calls, backup
restoration or deletion. Verify all original archived files byte-for-byte and
all five final private parents independently before recording active namespace
isolation. `ISOLATED` never changes the original FAIL or supplies canonical
acceptance. H2 recovery requires the completed H1 diagnostic and its successful
cleanup; it isolates all four H2 peers together when the H2 contender boundary
fails. In run 39733064688, the server refused the contender after a 1001
ms target write-lock timeout while the exact GET held its read lock for 4.148
s; the validator failed because it combined the same PUT's bucket-incarnation
read-lock events with the target write-lock events. Read-only inspection
[39738071753/1](https://github.com/rustfs/rustfs/actions/runs/39738071753)
observed vm000 with no RustFS service, validation unit or port 9000 listener.
Recovery [39738426191/1](https://github.com/rustfs/rustfs/actions/runs/39738426191)
completed `ISOLATED` for vm000: all 65 failed-artifact files match
byte-for-byte, preflight/move/post-audit physical digests match, and four data
roots plus private state are retained in five root-owned mode-0700 same-device
archive parents. No backup restoration, namespace deletion, S3 request or
original FAIL rewrite occurred. Inspection also observed old failed F07 units
on vm001-vm003; the I05 plan did not touch those nodes.

The first live run's GET already dropped its lock before observation. The
version-field correction correlates those original events but cannot reopen
that ended window. The exact candidate source and runtime log differ on the
optional version field; the cause remains unresolved. Any subsequent I05
controller must establish a real streaming/IO ownership window with evidence;
repeating the same warmed full-read premise does not provide that proof.

Source review narrows the next experiment without changing the oracle. The
candidate defaults the object-data cache off, and an explicit `versionId`
excludes the direct-memory GET path. The 32 MiB fixture is non-inline, while
codec streaming rollout is off by default, so this run used the legacy duplex
reader. `ObjectLockDiagGuard` carries the real namespace guard; the
`SetDiskLockGuardedReader` empty-read path can release that wrapper before the
HTTP response completes. The live journal shows `guard_dropped` before the
seven-millisecond HTTP completion and a later `get_object_stream_body`
`dropped_incomplete` event. This is a source-supported hypothesis for a
controlled streaming/IO reproduction, not a confirmed cause or a canonical
acceptance result.

I05 descriptor run `39754893467/1` pins helper
`dd7e54e6672cab76b5a6245ab3b4fbd051ddd39d` and the unchanged package from
`39364893106/1`. H1 retains its original FAIL/cleanup FAIL at the unchanged
30-second drain budget; H2 did not start. The baseline owns 22 descriptors.
Every drain resource snapshot owns 23, with all baseline fingerprints intact
and exactly one additional TCP descriptor from remote `168.63.129.16` to local
port 9000. The caller's exact GET socket is absent, and its read-lock journal
contains `guard_dropped`. These snapshots establish external connection
interference; they do not prove the platform probe configuration, continuous
socket absence, no leak, or complete canonical acceptance.

Preservation recognizes only this additional exact helper and its original
known 503 refusal, closed read-only GET, complete journal lifecycle, stopped
physical scene and bounded request/ACK set. The original 101-file artifact is
retained in `testdata/validation/i05-descriptor-drain-39754893467.zip`.
Descriptor counts, unique FD identities, socket-link fingerprints, owned TCP
joins, snapshot flags, process birth and network namespace must agree.
Missing census, unknown/successful mutation, extra request and physical drift
refuse preservation. Drain journal windows follow the numbered first-drain
label rather than a fixed count of preceding caller windows. This change
permits preservation only; it performs no network change and grants no PASS.

Isolation run [39757904633/1](https://github.com/rustfs/rustfs/actions/runs/39757904633)
completed with helper `f43151595648d83b184ab95fd3e908e7611d0dae`.
An independent artifact audit confirms all 101 failed-run files are byte exact,
preflight/move/post-audit baselines are identical, and all five archive parents
are root/root 0700 on their original devices. Original business-scene digest
`f921b69ea51622f89e9ad4a78de8c890caf0b7ffefa0b401457d6f9796fc7d31`
is unchanged. Only the owned active paths were isolated; no namespace was
deleted, no backup was restored and the original FAIL/cleanup FAIL remains.
The inventory now reports existing `bpftool` availability before selecting a
service-scoped network filter; this report installs no tool or filter.
An earlier dispatch `39757736435/1` was rejected before candidate resolution
because the submitted helper was abbreviated; no fleet action ran there.

Read-only inventory [39758842602/1](https://github.com/rustfs/rustfs/actions/runs/39758842602)
with helper `c5d1890467599e27aeffeb5dbf1ed4b37141fd06` confirms that all four
current fleet nodes already provide `bpftool` and cgroup v2, and none listens
on port 9000. This observation does not certify global idleness or historical
scene cleanup. Existing failed F07 units remain outside this operation.

The I05 `lock-timeout` runtime now starts its private transient systemd unit
with `IPAddressDeny=168.63.129.16/32` and no allow override. Startup and every
resource sample require exact effective properties, the owned MainPID's
membership in that unit's cgroup, both actual `sd_fw_ingress`/`sd_fw_egress`
attachments, and root-owned `cgroup_skb` program identities/tags from bpftool.
Missing support, ignored settings, foreign programs, aliased cgroups and
process changes fail closed. This policy isolates the observed external
source from this test service in both directions. Other units and the host
firewall are untouched; the policy has the private service cgroup's lifetime.
It does not identify the platform's probe configuration. Resource evidence
retains the effective properties and kernel program IDs/tags as snapshots.
The total FD threshold, caller/request identity and 30-second budget remain
unchanged; complete I05 canonical acceptance is still required separately.

Filter startup run [39761856012/1](https://github.com/rustfs/rustfs/actions/runs/39761856012)
used helper `a9725c88acd4977545b278eebe3653c916f907f8` and stopped at the
kernel attachment check, before seeding or issuing the diagnostic requests.
Its stopped H1 scene has four original format files and no object files; all
22 original artifact files and FAIL/cleanup FAIL remain retained. The parser
had accepted only the older bpftool `ingress`/`egress` attach type strings.
Newer bpftool uses libbpf's `cgroup_inet_ingress`/`cgroup_inet_egress` names for
the same kernel types. Both exact spellings now map to a checked direction,
while evidence preserves the original attach type. Unknown, duplicate or
missing directions and non-string flags still fail closed.

The startup preservation route requires that exact run/helper and all 22
original file hashes, original candidate/package/binary, stopped owner and
empty object scene. It reuses the existing same-device, private-parent
isolation backend and its live owner/physical/tree checks. No workload is
replayed and no namespace is deleted. A successful isolation or corrected
startup check cannot certify the I05 source case.

### I05 queued-successor diagnostic

Select `I05_DIAGNOSTIC_BRANCH=queued-successor` for the explicit I05 route.
`run_case.sh` passes this branch to both preflight and the runtime controller.
The Functional Chain input `i05_diagnostic_branch=queued-successor` downloads
the pinned native artifact and verifies its original hosted run, candidate,
helper, binary, exact regression logs and required I05 labels before fleet
mutation. `VALIDATION_HEAL_NATIVE_DIRECTORY` supplies that bundle locally.
Native success remains a prerequisite and never certifies the live case.

`lock_successor.py` uses a separate `lock-successor` runtime: the GET lock
budget remains one second, while the successor PUT commit acquisition budget
is sixty seconds. It starts exactly one marked PUT while the original
historical GET is admitted, observes the PUT acquiring the same namespace,
then closes only the original caller socket. It requires the original GET
guard-drop event before successor acquisition, an unretried server-request-ID
bound PUT ACK, exact ETag/version/payload, preserved historical generations,
a distinct new physical generation, all-peer S3 agreement and bounded FD
drain. Observation windows remain ten seconds for queue admission and thirty
seconds for completion. The fixture is a diagnostic subset of the source DV
workload; it does not complete the source DL/DV matrix.

Failure stops the run-owned service and retains its original requests, ACKs,
logs and physical scene. `successor-retention.json` binds all diagnostic files
and stopped per-node physical digests to the original owner. Recovery accepts
only an unchanged sealed archive and the same stopped live scene, then uses
the existing same-device private-parent isolation backend. An unknown PUT
outcome is preserved without retry, deletion or old-generation restoration.
Incomplete capture, missing reservations, changed evidence or scene drift
refuse automated isolation.

The exact-helper native/tools build
[39783997956/1](https://github.com/rustfs/rustfs/actions/runs/39783997956)
passed independent archive, source, executable and ten-regression audits.
The first successor live run
[39788321908/1](https://github.com/rustfs/rustfs/actions/runs/39788321908)
retains FAIL/cleanup FAIL: the same PUT read its bucket's internal
`.metadata.bin` after target write-lock acquisition; the controller
incorrectly classified that read as a target write. The request returned
one unretried 200 ACK, which does not complete the failed diagnostic.

[39789732916/1](https://github.com/rustfs/rustfs/actions/runs/39789732916)
isolated the unchanged stopped H1 scene without deleting it or restoring
old generations. The parser recognizes only that bucket's exact
`.metadata.bin` and `.bucket-incarnation` read paths, checks their original
HTTP identity, namespace, one-second budget and ordered per-attempt
lifecycle, and excludes them from the unique target write attempt. The
original fifteen-event trace is a regression fixture; foreign paths,
write-mode substitutions and reused or malformed lifecycles still refuse.
A new helper requires a newly bound native/tools build before another live
attempt. Fresh exact-helper native/tools
[39791663588/1](https://github.com/rustfs/rustfs/actions/runs/39791663588)
passed independent verification of all three official archives, 22 original
members, ten exact tests and 104 ordered milestones. The next live run
[39798325977/1](https://github.com/rustfs/rustfs/actions/runs/39798325977)
passed the metadata-read classifier and acknowledged one new PUT version,
then retained FAIL/cleanup FAIL because the controller invoked stopped-only
streaming shard verification while the service was running.

[39799657074/1](https://github.com/rustfs/rustfs/actions/runs/39799657074)
isolated that stopped H1 scene. Independent official archive verification
byte-matched all 71 original files and all five pre/move/post tree, device
and inode baselines. The new shard's streaming checksums now run only after
all online peer reads and FD observations, under the unchanged stopped-only
verifier. Stopped physical snapshots must match the ACK-bound pre-read scene
both before and after the checksum pass. Drift or checksum failure retains
the original scene and prevents cleanup. Exact helper `6fbc3c4` tools
[39803149039/1](https://github.com/rustfs/rustfs/actions/runs/39803149039)
passed independent audits of all three official ZIPs, 22 original members,
ten exact native tests, 104 ordered milestones and inspector contracts.
Live [39810035277/1](https://github.com/rustfs/rustfs/actions/runs/39810035277)
completed the new H1 stopped-integrity sequence and cleanup, then retained
H2 because a slow-acquire WARN sharing the diagnostic target was parsed as
a lifecycle event. The WARN has no event/state/attempt/namespace fields;
it reports a 1214 ms acquisition against a 500 ms warning threshold. It is
not a timeout or a separate write attempt. The selector recognizes this
exact auxiliary shape only after the same request's acquired target write,
retaining strict HTTP identity, owner, numeric timing and lifecycle checks.
Unknown warnings and lifecycle substitutions still refuse; the auxiliary
warning cannot establish acquisition or guard drop.

Isolation [39811143272/1](https://github.com/rustfs/rustfs/actions/runs/39811143272)
preserved all 180 original failed files without modification. Independent
archive audit verified all four nodes' pre/move/post tree, device and inode
baselines and twenty root-owned 0700 archive parents on their original
devices. No S3 replay, namespace deletion, old-generation restoration or
original verdict rewrite occurred.

Exact helper `3012a65` tools
[39812960916/1](https://github.com/rustfs/rustfs/actions/runs/39812960916)
passed independent verification of three official ZIPs, 22 original members,
ten exact native tests, 104 ordered milestones, actual hosted checkouts,
source/tree/Cargo.lock, optimized ELF and inspector quorum/size contracts.
Fresh live
[39817606025/1](https://github.com/rustfs/rustfs/actions/runs/39817606025)
completed H1 and H2 with five passing node cleanup receipts. Independent
audit byte-matched all 246 official files and verified nine seed request/ACK
pairs, eight final versions/markers per topology, one unretried successor
PUT ACK, retained historical physical generations, actual lock lifecycle
events and every peer's complete inventory. Stopped physical snapshots match
the ACK-bound scene before and after all four H1 and sixteen H2 new-shard
streaming receipts. Final FD counts were H1 vm000 `22 -> 22`; H2 vm000
`108 -> 96`, vm001 `117 -> 113`, vm002 `105 -> 101`, vm003 `108 -> 104`.
Both diagnostic branches are independently accepted, with the original
`BLOCKED/cleanup PASS` unchanged. Precise disconnect-edge ownership,
distributed release and global absence of leaks are explicitly unproven;
the complete source DL/DV fixtures were not covered.
Every successful diagnostic
remains `BLOCKED/cleanup PASS` with `canonicalCasePassClaimed=false`. Exact
disconnect-edge ownership, complete native timeout/stale fences, the
stale-generation controller and full H1/H2 source DL/DV ACK/physical/lock/FD
closure remain required. Migration acceptance stays 108/158.
