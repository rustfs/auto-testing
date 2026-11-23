# Strict SSE contract cases

[中文](sse-contract-cases.zh-CN.md)

`scripts/sse_contract_cases.py` holds nineteen cases that state what a client must observe from server-side encryption. The cluster suites register them next to their older encryption cases and report them through the same `[PASS]`, `[FAIL]` and case-table lines.

## What "strict" means

- **Exact SSE headers.** A response is reduced to its headers starting with `x-amz-server-side-encryption`, and that set must equal the expected one: nothing missing and nothing extra. SSE-S3 is `{AES256}`, SSE-KMS is `aws:kms` plus the key id the request resolved to, SSE-C is the customer algorithm plus the key MD5, and a plaintext object has none.
- **Exact refusals.** A refusal must carry the expected HTTP status and S3 error code. A pattern such as `400|InvalidArgument` is not used.
- **No residue.** A refused write is followed by a HEAD of the same key, which must answer 404.
- **Bytes.** Data read back is compared by SHA-256 and by `Content-Length`.
- **No vacuous pass.** A case that evaluated no expectation fails.

A table-driven case names each input row in its failure lines, and a row that breaks does not stop the next row.

## Cases in `rustfs-s3-compat-test.sh`

The server of this suite has `RUSTFS_SSE_S3_MASTER_KEY` and no KMS. `ENC-204` depends on that: with KMS running, SSE-KMS is accepted and the case fails.

| Case | Name | Holds when |
| --- | --- | --- |
| `ENC-201` | SseS3ExactHeaders | SSE-S3 objects of 0, 1 and 71680 bytes answer PUT, GET and HEAD with exactly x-amz-server-side-encryption: AES256, read back byte-identical, and are listed with their plaintext size. |
| `ENC-202` | SseCExactHeaders | SSE-C objects of 0, 1 and 71680 bytes answer PUT, GET and HEAD with exactly the customer algorithm and key-MD5 headers, and a GET without the customer key is refused with 400 InvalidRequest. |
| `ENC-203` | BucketDefaultExact | A bucket default of AES256 encrypts a PUT that names no scheme, leaves an SSE-C PUT as SSE-C, and after its removal new objects carry no SSE header while earlier ones still read back as SSE-S3. |
| `ENC-204` | SseKmsNotDowngraded | Without KMS an SSE-KMS PUT is refused with 400 InvalidRequest and stores nothing, while SSE-S3 under the master key still round-trips. |
| `ENC-205` | SseCTripleNegatives | Seven incomplete or inconsistent SSE-C header triples are each refused with 400 InvalidRequest, and a HEAD of each key answers 404. |
| `ENC-206` | SseCCopySourceKey | CopyObject from an SSE-C source without or with a wrong source key is refused with an explicit 400 and stores nothing; with the right key the destination opens only with its own key. |
| `ENC-207` | SseCMultipartExact | An SSE-C multipart upload answers CreateMultipartUpload and CompleteMultipartUpload with exactly the customer headers, reads back byte-identical with the key, and is refused without it. |
| `ENC-208` | SseCPartKeyMismatch | UploadPart without the key, with another key, or with a key on a plaintext upload is refused with 400 InvalidRequest, and the SSE-C upload still completes afterwards. |
| `ENC-209` | SseReadHeaderMismatch | Six GET requests whose SSE headers do not match how the object was stored are refused with the exact error code and return no object bytes. |
| `ENC-210` | ListingEtagNotPlaintextMd5 | ListObjectsV2, ListObjects and ListObjectVersions never report the MD5 of the plaintext as the ETag of an SSE-S3 or SSE-C object, with a plaintext object as the control. |

## Cases in `rustfs-kms-test.sh`

The server of this suite runs KMS with the default key `kms-default` on the Local or Vault KV2 backend. The helper receives the default key id through `--kms-default-key` and creates `sse-contract-key-1` and `sse-contract-key-2` through the admin API before a case starts; a key that already exists is reused. The cases run after `KMS-318` and before the per-key authorization and fault cases.

| Case | Name | Holds when |
| --- | --- | --- |
| `KMS-601` | SseS3KeyNotNamed | SSE-S3 under KMS round-trips with exactly AES256, and no response of eight operations carries a KMS key id header or names the default key. |
| `KMS-602` | SseKmsKeyResolutionExact | SSE-KMS without a key id answers with the default key id, and with an explicit non-default key answers with that key id on PUT, GET and HEAD for 0, 1 and 71680 bytes. |
| `KMS-603` | SseKmsRangeExact | Four range reads of an SSE-KMS object answer 206 with the exact Content-Range, the plaintext slice and the exact SSE-KMS headers. |
| `KMS-604` | BucketDefaultKmsExact | A bucket default of aws:kms without a key id pins the default key, leaves SSE-C as SSE-C, and an explicit AES256 PUT does not inherit the bucket's KMS key. |
| `KMS-605` | CopySchemeMatrix | Six CopyObject combinations across plaintext, SSE-S3, SSE-KMS and SSE-C produce exactly the destination scheme's headers on GET and HEAD and byte-identical data. |
| `KMS-606` | MultipartSseKmsExact | A three-part SSE-KMS upload answers create, complete, GET and HEAD with exactly the SSE-KMS headers of its key, the full length and a three-part ETag. |
| `KMS-607` | SsePutHeaderNegatives | Ten malformed SSE header combinations are refused with 400 InvalidArgument and a missing KMS key with 400 KMS.NotFoundException; a HEAD of each key answers 404. |
| `KMS-608` | SseReadHeaderMismatch | Seven GET requests whose SSE headers do not match how the object was stored, an SSE-KMS object included, are refused with the exact error code and return no object bytes. |
| `KMS-609` | ContextAndEtagNotEchoed | PUT, GET and HEAD of an object written with an encryption context do not repeat the context, and no listing reports the plaintext MD5 as the ETag of an encrypted object. |

## Running one case

```bash
./rustfs-s3-compat-test.sh --topology single-single -y --cases ENC-204 --package-url <deb url>
./rustfs-kms-test.sh --topology single-single --backends local -y --cases KMS-605 --package-url <deb url>

RUSTFS_ACCESS_KEY=... RUSTFS_SECRET_KEY=... \
  python3 scripts/sse_contract_cases.py --endpoint http://node:9000 --case KMS-605 --kms-default-key kms-default
```

The helper needs Python 3.8 or newer and the standard library only. It connects to the endpoint directly and ignores proxy variables. Each case works in buckets named `sse-<case id>-<random>` and removes them afterwards on a best-effort basis. Exit status: 0 the case holds, 1 it does not (an unreachable endpoint included), 64 the command line is wrong. Status 2 is never used, because `run_test` reads it as UNSUPPORTED.

The catalog ids are `s3.compatibility.enc-2xx` and `source-procedure.kms.kms-6xx`, so `--validation-case` selects a single case in the same way as for the older ones.

## What has been verified

- Every case holds against a local single-process server started with the environment the respective suite writes: master key only for `ENC-2xx`; Local and Vault KV2 KMS for `KMS-6xx`, also with `RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true` and `RUSTFS_ENCRYPTION_FRAME_V2=true`.
- The suites' own `run_all_tests` ran the cases against those servers and printed the expected `[PASS]` lines; against a server without a master key `ENC-201` and `ENC-204` printed `[FAIL]` with the refusal they saw.
- `test_sse_contract_cases.py` runs every case against an in-memory S3 that behaves as required, and again with one behavior wrong at a time; the matching case must fail and name what it saw.

The cases have not run on the fleet yet. Multi-node topologies and the packaged binary are covered only once the scheduled suites pick the cases up.

## What is deliberately not here

The cases were taken from a suite that drives a bare server binary. Parts of it need things a deployed cluster cannot give and were left out: starting the server with other key sources (none at all, an invalid master key, Vault Transit, the Static backend), comparing two binaries, checking the stored form of objects on the drives, and searching whole runs for key material. Per-key authorization with separate identities is covered loosely by `KMS-401` and `KMS-402` and is not tightened here.
