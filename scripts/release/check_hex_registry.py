#!/usr/bin/env python3
"""Verify a gzipped Hex registry package from stdin against an archive digest."""

import argparse
import gzip
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import zlib

# The public key is Hex v2.7.0's default for repo.hex.pm:
# https://github.com/hexpm/hex/blob/92cb6a1b10c9b008ccb6fa229f5d9916840dc09d/lib/hex/repo.ex#L4-L15
PUBLIC_KEY = Path(__file__).with_name("hexpm-registry-public-key.pem")
MAX_REGISTRY_SIZE = 16 * 1024 * 1024

# Hex Core's Signed and Package schemas and RSA PKCS#1 v1.5/SHA-512 verification:
# https://github.com/hexpm/hex_core/blob/981ea0ef871833944ccaf94d8207121c48f7ada3/proto/hex_pb_signed.proto
# https://github.com/hexpm/hex_core/blob/981ea0ef871833944ccaf94d8207121c48f7ada3/proto/hex_pb_package.proto
# https://github.com/hexpm/hex_core/blob/981ea0ef871833944ccaf94d8207121c48f7ada3/src/hex_registry.erl#L178-L199


def varint(data, offset):
    value = 0
    for shift in range(0, 64, 7):
        if offset == len(data) or (shift == 63 and data[offset] > 1):
            break
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if byte < 0x80:
            return value, offset
    raise ValueError("invalid protobuf varint")


def fields(data):
    result = {}
    offset = 0
    while offset < len(data):
        tag, offset = varint(data, offset)
        field, wire = divmod(tag, 8)
        if not 0 < field < 2**29:
            raise ValueError("invalid protobuf field number")
        if wire == 0:
            _, end = varint(data, offset)
        elif wire == 2:
            size, offset = varint(data, offset)
            end = offset + size
        elif wire in (1, 5):
            end = offset + (8 if wire == 1 else 4)
        else:
            raise ValueError("unsupported protobuf wire type")
        if end > len(data):
            raise ValueError("truncated protobuf field")
        result.setdefault(field, []).append(data[offset:end] if wire == 2 else None)
        offset = end
    return result


def required_bytes(message, field):
    values = message.get(field, [])
    if len(values) != 1 or not values[0]:
        raise ValueError(f"expected one nonempty protobuf bytes field {field}")
    return values[0]


def verify_signature(payload, signature):
    with TemporaryDirectory(prefix="hex-registry-") as directory:
        signature_file = Path(directory) / "signature"
        signature_file.write_bytes(signature)
        result = subprocess.run(
            ["openssl", "dgst", "-sha512", "-verify", str(PUBLIC_KEY),
             "-sigopt", "rsa_padding_mode:pkcs1", "-signature", str(signature_file)],
            input=payload, capture_output=True, timeout=10,
        )
    if result.returncode:
        raise ValueError("Hex registry signature verification failed")


def check(compressed, version, checksum):
    with gzip.GzipFile(fileobj=compressed) as stream:
        signed = stream.read(MAX_REGISTRY_SIZE + 1)
    if len(signed) > MAX_REGISTRY_SIZE:
        raise ValueError("Hex registry exceeds the uncompressed size limit")
    envelope = fields(signed)
    payload = required_bytes(envelope, 1)
    verify_signature(payload, required_bytes(envelope, 2))
    check_package(payload, version, checksum)


def check_package(payload, version, checksum):
    package = fields(payload)
    if required_bytes(package, 2) != b"replicant" or required_bytes(package, 3) != b"hexpm":
        raise ValueError("signed registry package does not name hexpm/replicant")
    matching = []
    expected_version = version.encode("utf-8")
    for record in package.get(1, []):
        if record is None:
            raise ValueError("invalid registry Release field")
        release = fields(record)
        if required_bytes(release, 1) == expected_version:
            matching.append(release)
    if len(matching) != 1:
        raise ValueError(f"expected exactly one registry release for replicant {version}")
    release = matching[0]
    if 4 in release:
        raise ValueError(f"Hex registry lists retired replicant {version}")
    if required_bytes(release, 5) != checksum:
        raise ValueError(f"outer checksum differs for replicant {version}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("sha256")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{64}", args.sha256):
        parser.error("expected a full lowercase SHA-256 checksum")
    try:
        check(sys.stdin.buffer, args.version, bytes.fromhex(args.sha256))
    except (EOFError, OSError, ValueError, subprocess.TimeoutExpired, zlib.error) as error:
        print(f"Hex registry refused: {error}", file=sys.stderr)
        return 1
    print(f"Signed Hex registry matches replicant {args.version} and its outer checksum")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
