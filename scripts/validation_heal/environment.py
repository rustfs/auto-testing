#!/usr/bin/env python3
"""Read the current Heal fleet's capabilities; never execute or score a case."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

try:
    from .heal_probe import require, save
    from .healthy import Fleet
except ImportError:
    from heal_probe import require, save
    from healthy import Fleet


def eligible_mounts(value):
    rows = value.get('mounts')
    require(value.get('inspectionOnly') is True and value.get('mutated') is False
            and value.get('canonicalPassClaimed') is False and isinstance(rows, list) and len(rows) == 8,
            'incomplete or mutating environment inspection')
    require({row.get('target') for row in rows} == {f'/data/rustfs{number}' for number in range(1, 9)},
            'environment inspection duplicated or omitted a mount endpoint')
    eligible = {}
    for row in rows:
        if not row['mounted']: continue
        mount = row.get('mount') or {}
        if (row.get('independentBlockDeviceVerified') is True and row.get('writableMount') is True
                and mount.get('uuid') and mount.get('target') == row['target']
                and mount.get('fstype') in {'xfs', 'ext4'} and row.get('availableBytes', 0) >= 2 * 1024**3):
            eligible[row['target']] = row
    # Two partitions from the same disk are one failure domain. Aliases or
    # duplicated filesystem IDs cannot be counted as independent endpoints.
    physical = [row['physicalDevice'] for row in eligible.values()]
    identifiers = [row['mount']['uuid'] for row in eligible.values()]
    require(len(physical) == len(set(physical)) and len(identifiers) == len(set(identifiers)),
            'environment inspection found aliased physical failure domains')
    return eligible


def summarize(observed):
    require(len(observed) == 4 and len({value.get('hostname') for value in observed.values()}) == 4
            and all(value.get('hostname') for value in observed.values()), 'inspection requires four actual distinct current Heal hosts')
    mounts = {node: eligible_mounts(value) for node, value in observed.items()}
    identifiers = [row['mount']['uuid'] for rows in mounts.values() for row in rows.values()]
    require(len(identifiers) == len(set(identifiers)), 'environment inspection found shared or cloned filesystem identities')
    nodes = list(observed)
    available = lambda selected, count: all({f'/data/rustfs{number}' for number in range(1, count + 1)} <= set(mounts[node])
                                           for node in selected)
    return {'independentEligibleEndpoints': sum(len(rows) for rows in mounts.values()),
        'eligibleEndpointsPerNode': {node: sorted(rows) for node, rows in mounts.items()},
        'resourceShapes': {'H1': {'mountsAvailable': available(nodes[:1], 4), 'requiredEndpoints': 4},
                           'H2': {'mountsAvailable': available(nodes, 4), 'requiredEndpoints': 16},
                           'H3': {'mountsAvailable': available(nodes[:3], 4), 'requiredEndpoints': 12},
                           'H4': {'mountsAvailable': available(nodes[:3], 8), 'requiredEndpoints': 24},
                           'H6': {'platformRestartAuthorityVerified': False}},
        'runtimeObservationsComplete': all(value.get('runtime', {}).get('stableObservation') is True
                                           for value in observed.values()),
        'cleanStateCertified': False, 'globalWorkflowIdleCertified': False,
        'actualErasureLayoutVerified': False, 'faultBackendVerified': False,
        'compatibleReleasePairVerified': False, 'canonicalPassClaimed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    require(subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip() == args.testing_commit
            and re.fullmatch(r'[0-9a-f]{40}', args.testing_commit), 'environment inspection requires an exact harness checkout')
    candidate = json.loads(args.candidate.read_text())
    require(re.fullmatch(r'[0-9a-f]{40}', candidate['source_sha']) and re.fullmatch(r'[0-9a-f]{64}', candidate['package_sha256']),
            'environment inspection has an invalid candidate identity')
    workflow = os.environ.get('GITHUB_RUN_ID', '')
    attempt = os.environ.get('GITHUB_RUN_ATTEMPT', '1')
    require(re.fullmatch(r'[1-9][0-9]*', workflow) and re.fullmatch(r'[1-9][0-9]*', attempt), 'environment inspection requires workflow provenance')
    nodes = (os.environ.get('RUSTFS_NODES') or 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'invalid current Heal node binding')
    user = os.environ.get('RUSTFS_SSH_USER') or 'azureuser'
    require(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', user), 'invalid current Heal SSH user')
    os.environ['RUSTFS_SSH_USER'] = user
    args.evidence.mkdir(parents=True, exist_ok=False)
    fleet = Fleet(nodes[:4], args.evidence, f'gh-{workflow}-{attempt}-h2')
    result = {'schemaVersion': 1, 'runId': workflow, 'runAttempt': attempt, 'testingCommit': args.testing_commit,
        'candidate': candidate, 'inspectionSourceSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'inspectionOnly': True, 'mutated': False, 'canonicalPassClaimed': False, 'complete': False}
    try:
        observed = {node: fleet.record('node-capabilities', {'node': node, **fleet.remote(node, 'environment-capabilities')})
                    for node in fleet.nodes}
        result.update(nodes=observed, resources=summarize(observed), complete=True)
    finally:
        save(args.evidence / 'environment-summary.json', result)
    print(json.dumps(result['resources'], sort_keys=True))


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'environment inspection failed: {exc}', file=sys.stderr)
        sys.exit(1)
