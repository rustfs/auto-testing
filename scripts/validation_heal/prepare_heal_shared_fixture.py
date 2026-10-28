#!/usr/bin/env python3
"""Prepare the bounded C10 metadata encoder against one unmodified candidate."""

import argparse
import hashlib
import json
from pathlib import Path

try:
    from .prepare_heal_meta_inspector import prepare
except ImportError:
    from prepare_heal_meta_inspector import prepare


ROLE = 'controlled C10 shared-reference metadata encoder; not service under test'


def prepare_shared(source, commit, output):
    prepare(source, commit, output)
    helper = Path(__file__).with_name('tools') / 'heal_shared_version_fixture.rs'
    (output / 'src/main.rs').write_bytes(helper.read_bytes())
    manifest = output / 'Cargo.toml'
    manifest.write_text(manifest.read_text().replace('name = "heal-meta-inspect"', 'name = "heal-shared-fixture"'))
    with manifest.open('a') as stream:
        stream.write('[dev-dependencies]\ntime = "0.3"\n')
    path = output / 'source.json'
    proof = json.loads(path.read_text())
    proof.update(role=ROLE, helperSourceSha256=hashlib.sha256(helper.read_bytes()).hexdigest(),
        filesystemMutationPerformed=False, physicalByteEqualityClaimed=False)
    path.write_text(json.dumps(proof, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    prepare_shared(args.source, args.commit, args.out)
