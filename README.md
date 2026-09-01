# auto-testing
Auto testing scripts

## Tier suite artifacts

`rustfs-tier-test.sh` can persist machine-readable evidence with
`--artifacts-dir <unique-directory>`. The directory contains one atomic JSON
result per `(topology, case_id)`, the exact `rc` path/version/SHA-256 and
isolated config directory in `provenance.json`, failed-case logs, and a
`residue.jsonl` cleanup manifest.

Use `rustfs_tier_report.py` as the final gate. A complete two-topology run must
contain 56 unique terminal results; FAIL, RUNNING, missing, duplicate, corrupt,
or non-allowlisted UNSUPPORTED results make the command exit non-zero while it
still writes the Markdown report.

```bash
./rustfs-tier-test.sh --all-topologies -y \
  --package-url https://example.invalid/rustfs.deb \
  --rc-bin /absolute/path/to/rc \
  --expected-rc-sha256 <sha256> \
  --artifacts-dir /tmp/rustfs-tier-artifacts-unique

python3 rustfs_tier_report.py \
  --results-dir /tmp/rustfs-tier-artifacts-unique/cases \
  --provenance /tmp/rustfs-tier-artifacts-unique/provenance.json \
  --output /tmp/rustfs-tier-report.md
```

Local contract tests:

```bash
./test_rustfs_tier.sh
python3 -m unittest -v test_rustfs_tier_report.py
```
