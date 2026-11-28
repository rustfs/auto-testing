#!/usr/bin/env python3
"""Exercise S3C-305 against deterministic multipart wire responses."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).with_name("rustfs-s3-compat-test.sh")
MIB = 1024 * 1024
FAKE_ENDPOINT = r'''
from pathlib import Path
import json
import sys
from xml.etree import ElementTree

fixture, endpoint_trace, mutation, method, path, header_flag, header, data_flag, data = sys.argv[1:]
assert (method, header_flag, header, data_flag) == ("POST", "-H", "Content-Type: application/xml", "--data")
request = ElementTree.parse(data).getroot()
assert request.tag.rsplit("}", 1)[-1] == "CompleteMultipartUpload"
key = path.split("?")[0].rsplit("/", 1)[-1]
assert path.endswith(f"?uploadId={key}")
directory = Path(fixture)
numbers, sizes, sources = [], [], []
for part in request.findall("{*}Part"):
    number = int(part.findtext("{*}PartNumber"))
    assert part.findtext("{*}ETag") == f'"{key}-{number}"'
    source = directory / f"{key}-{number}"
    numbers.append(number)
    sizes.append(source.stat().st_size)
    sources.append(source)
assert numbers

status, code = 200, None
if numbers != sorted(numbers):
    status, code = 400, "InvalidPartOrder"
elif any(size < 5 * 1024 * 1024 for size in sizes[:-1]):
    status, code = 400, "EntityTooSmall"
    if mutation == "accept-non-final":
        status, code = 200, None
    elif mutation == "wrong-status":
        status = 500
    elif mutation == "wrong-code":
        code = "InvalidPart"
elif key == "small-final" and mutation == "embedded-error":
    code = "InternalError"

if code:
    body = ElementTree.Element("Error")
    ElementTree.SubElement(body, "Code").text = code
    ElementTree.SubElement(body, "Message").text = "EntityTooSmall"
else:
    body = ElementTree.Element("CompleteMultipartUploadResult")
    ElementTree.SubElement(body, "Key").text = key
    ElementTree.SubElement(body, "ETag").text = '"assembled"'
    with (directory / (key + ".object")).open("wb") as assembled:
        for source in sources:
            assembled.write(source.read_bytes())
        if key == "small-only" and mutation == "corrupt-control":
            assembled.write(b"!")
ElementTree.ElementTree(body).write(directory / "curl.out")
with open(endpoint_trace, "a", encoding="utf-8") as log:
    log.write(json.dumps({"key": key, "parts": numbers, "sizes": sizes, "status": status, "code": code}) + "\n")
print(status, end="")
'''

HARNESS = r'''
source "$RUNNER"
make_test_file() {
  "$PYTHON" - "$1" "$2" <<'FIXTURE'
from pathlib import Path
import sys
path, size = sys.argv[1:]
Path(path).write_bytes((b"L" if path.endswith("-large") else b"S") * int(size))
FIXTURE
}
aws_run() {
  local operation="$2" key="" number="" body="" previous="" argument
  for argument in "$@"; do
    case "${previous}" in
      --key) key="$argument" ;;
      --part-number) number="$argument" ;;
      --body) body="$argument" ;;
    esac
    previous="$argument"
  done
  AWS_CODE=0
  AWS_OUT='{}'
  case "${operation}" in
    create-bucket|delete-bucket|delete-object|abort-multipart-upload) ;;
    create-multipart-upload) AWS_OUT="{\"UploadId\":\"${key}\"}" ;;
    upload-part)
      cp "$body" "$TEST_TMP/${key}-${number}"
      AWS_OUT="{\"ETag\":\"\\\"${key}-${number}\\\"\"}"
      ;;
    head-object)
      if [ ! -f "$TEST_TMP/${key}.object" ]; then
        AWS_CODE=254
        AWS_OUT='An error occurred (404) when calling the HeadObject operation: Not Found'
      fi
      ;;
    get-object) cp "$TEST_TMP/${key}.object" "${@: -1}" ;;
    *) echo "unexpected AWS operation: ${operation}"; return 1 ;;
  esac
}
s3_signed_curl() {
  "$PYTHON" "$FAKE_ENDPOINT" "$TEST_TMP" "$ENDPOINT_TRACE" "$MUTATION" "$@"
}
test_s3c305
'''


@unittest.skipUnless(shutil.which("bash") and shutil.which("jq"), "requires Bash and jq")
class TestMultipartCompletionContract(unittest.TestCase):
    def run_case(self, mutation="none"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            endpoint = root / "endpoint.py"
            trace = root / "requests.jsonl"
            endpoint.write_text(FAKE_ENDPOINT)
            env = dict(os.environ, RUNNER=str(RUNNER), PYTHON=sys.executable,
                       FAKE_ENDPOINT=str(endpoint), ENDPOINT_TRACE=str(trace),
                       MUTATION=mutation, TMPDIR=directory)
            result = subprocess.run(["bash", "-c", HARNESS], env=env, capture_output=True, text=True, timeout=30)
            requests = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
            return result, requests

    def test_ineligible_and_eligible_manifests(self):
        result, requests = self.run_case()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            [(request["key"], request["parts"], request["sizes"], request["status"], request["code"])
             for request in requests],
            [("out-of-order", [2, 1], [5 * MIB, 5 * MIB], 400, "InvalidPartOrder"),
             ("small-non-final", [1, 2], [MIB, 5 * MIB], 400, "EntityTooSmall"),
             ("small-final", [1, 2], [5 * MIB, MIB], 200, None),
             ("small-only", [1], [MIB], 200, None)],
        )
        self.assertIn("small-final: assembled bytes match", result.stdout)
        self.assertIn("small-only: assembled bytes match", result.stdout)

    def test_non_final_small_part_requires_exact_http_and_error_code(self):
        for mutation in ("accept-non-final", "wrong-status", "wrong-code"):
            with self.subTest(mutation=mutation):
                result, requests = self.run_case(mutation)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("small-non-final: expected HTTP 400 with S3 error EntityTooSmall", result.stderr)
                self.assertEqual(len(requests), 4)

    def test_http_200_with_an_embedded_error_is_not_success(self):
        result, _ = self.run_case("embedded-error")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("small-final: HTTP 200 Error InternalError", result.stdout)

    def test_single_small_part_requires_the_original_bytes(self):
        result, _ = self.run_case("corrupt-control")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("small-only: assembled object differs from its parts", result.stdout)


if __name__ == "__main__":
    unittest.main()
