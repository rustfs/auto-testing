#!/usr/bin/env python3
"""Build the XFS test image with a digest-verified official GNU release binary."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import zipfile


def prepare(release, platform, output, tag):
    architecture = {'linux/amd64': 'x86_64', 'linux/arm64': 'aarch64'}[platform]
    metadata = json.loads(subprocess.check_output(['gh', 'release', 'view', release,
        '--repo', 'rustfs/rustfs', '--json', 'tagName,assets'], text=True))
    name = f'rustfs-linux-{architecture}-gnu-v{release.removeprefix("v")}.zip'
    asset = next(row for row in metadata['assets'] if row['name'] == name)
    expected = asset.get('digest', '').removeprefix('sha256:')
    if len(expected) != 64:
        raise ValueError('official release asset lacks a SHA-256 digest')
    output.mkdir(parents=True, exist_ok=True)
    archive = output / 'rustfs.zip'
    if not archive.exists() or hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
        subprocess.run(['curl', '--fail', '--location', '--retry', '2', '--continue-at', '-',
                        '--output', str(archive), asset['url']], check=True)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
        raise ValueError('official release archive digest differs')
    with zipfile.ZipFile(archive) as package:
        binaries = [row for row in package.infolist() if Path(row.filename).name == 'rustfs' and not row.is_dir()]
        if len(binaries) != 1:
            raise ValueError('official archive must contain exactly one RustFS binary')
        with package.open(binaries[0]) as source, (output / 'rustfs').open('wb') as target:
            shutil.copyfileobj(source, target)
    (output / 'rustfs').chmod(0o755)
    source = Path(__file__).resolve().parent
    for relative in ('node_replacement_node.py', 'node-replacement.Dockerfile'):
        shutil.copyfile(source / relative, output / relative)
    (output / '.dockerignore').write_text('*\n!rustfs\n!candidate.json\n!node_replacement_node.py\n!node-replacement.Dockerfile\n')
    manifest = {'release': metadata['tagName'], 'platform': platform, 'assetUrl': asset['url'],
                'archiveSha256': expected, 'binarySha256': hashlib.sha256((output / 'rustfs').read_bytes()).hexdigest()}
    (output / 'candidate.json').write_text(json.dumps(manifest, indent=2) + '\n')
    subprocess.run(['docker', 'build', '--platform', platform, '-f', str(output / 'node-replacement.Dockerfile'),
                    '-t', tag, str(output)], check=True)
    print(json.dumps(manifest, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', default='1.0.5')
    parser.add_argument('--platform', choices=('linux/amd64', 'linux/arm64'), default='linux/amd64')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--tag', default='rustfs-node-replacement-xfs:local')
    args = parser.parse_args()
    prepare(args.release, args.platform, args.out, args.tag)


if __name__ == '__main__':
    main()
