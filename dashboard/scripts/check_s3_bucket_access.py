"""
S3 Access Tool - inspect what a credential pair can do on an S3 endpoint.

Interactive menu tool. Start it with:

    python dashboard/scripts/check_s3_bucket_access.py

    --- S3 Access Tool ---
    1) Set Credentials
    2) Full Scan
    3) Access Check
    4) Inspect Access Policy
    5) Exit

    Enter your choice:

- ``Set Credentials`` asks for the access key, secret key and endpoint URL
  from the console. The values are kept in memory for the session only and
  are never written to disk.
- ``Full Scan`` lists every visible bucket and, per bucket, reports whether it
  is reachable, its top-level structure and its contents.
- ``Access Check`` asks for a user name (bucket) and runs the pass/fail
  checks for that target.
- ``Inspect Access Policy`` asks for a user name (bucket) and shows its access
  model: bucket policy (the principals it grants access to), ACL, policy
  status and public access block.
- Every operation returns to the main menu.

The tool is intended for manual and pre-deployment verification, not as a
pytest unit test.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Allow running this file directly via "python dashboard/scripts/check_s3_bucket_access.py".
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REQUIRED_CHECKS = ("credentials_valid", "bucket_reachable", "list_objects", "read_object")


@dataclass
class Credentials:
    """A single S3 credential pair plus the endpoint it talks to."""

    access_key: str
    secret_key: str
    endpoint_url: str

    def present(self) -> bool:
        """Return True when every credential field is non-empty."""
        return bool(self.access_key and self.secret_key and self.endpoint_url)

    def masked(self) -> str:
        """Render the pair for display with the secret masked."""
        return f"access_key={_mask_secret(self.access_key)}, endpoint={self.endpoint_url}"


# Credentials for the interactive session; set via the menu, kept in memory only.
_session_credentials: Credentials | None = None


def _mask_secret(value: str | None) -> str:
    """Render a secret value masked for safe display in logs and reports."""
    if not value:
        return "<missing>"
    if len(value) <= 6:
        return "***"
    return f"{value[:3]}***{value[-3:]}"


def _read_line(prompt: str) -> str:
    """Read a line from stdin, tolerating closed or non-interactive input."""
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        return ""


def _print_status(name: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"{status}: {name} - {detail}")


def _make_s3_client(creds: Credentials):
    """Build an S3 client for the given in-memory credentials."""
    return boto3.client(
        "s3",
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        endpoint_url=creds.endpoint_url,
    )


def _check_credentials_present(creds: Credentials | None) -> bool:
    """Verify that all credential fields are non-empty."""
    if creds is None:
        _print_status("credentials_present", False, "No credentials set.")
        return False
    missing = [
        field
        for field in ("access_key", "secret_key", "endpoint_url")
        if not getattr(creds, field)
    ]
    if missing:
        _print_status(
            "credentials_present",
            False,
            f"Missing credential fields: {', '.join(missing)}.",
        )
        return False
    _print_status(
        "credentials_present",
        True,
        f"access_key={_mask_secret(creds.access_key)}, "
        f"endpoint={creds.endpoint_url}.",
    )
    return True


def _check_credentials_valid(s3) -> bool:
    """Verify that the endpoint accepts the credentials via list_buckets."""
    try:
        buckets = s3.list_buckets()["Buckets"]
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        _print_status(
            "credentials_valid",
            False,
            f"Endpoint rejected credentials ({error_code}: {exc}).",
        )
        return False
    except Exception as exc:  # noqa: BLE001
        _print_status(
            "credentials_valid",
            False,
            f"Endpoint unreachable ({type(exc).__name__}: {exc}).",
        )
        return False

    bucket_names = [bucket["Name"] for bucket in buckets]
    _print_status(
        "credentials_valid",
        True,
        f"Credentials accepted; {len(bucket_names)} visible buckets.",
    )
    return True


def _check_bucket_reachable(s3, bucket: str) -> bool:
    """Verify that the bucket exists and the credentials can reach it."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        hint = "Bucket does not exist." if error_code == "404" else "Access denied."
        _print_status(
            "bucket_reachable",
            False,
            f"{hint} ({error_code}: {exc}).",
        )
        return False
    except Exception as exc:  # noqa: BLE001
        _print_status(
            "bucket_reachable",
            False,
            f"Bucket check failed ({type(exc).__name__}: {exc}).",
        )
        return False

    _print_status("bucket_reachable", True, f"Bucket '{bucket}' reachable.")
    return True


def _check_list_objects(s3, bucket: str) -> bool:
    """Verify ListBucket permission and report a sample of the listing."""
    try:
        response = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        _print_status(
            "list_objects",
            False,
            f"Cannot list objects ({error_code}: {exc}).",
        )
        return False
    except Exception as exc:  # noqa: BLE001
        _print_status(
            "list_objects",
            False,
            f"Listing failed ({type(exc).__name__}: {exc}).",
        )
        return False

    contents = response.get("Contents", [])
    truncated = response.get("IsTruncated", False)
    if not contents:
        _print_status(
            "list_objects",
            True,
            "Bucket reachable for listing (whole bucket); no objects found.",
        )
        return True

    sample = ", ".join(obj["Key"] for obj in contents[:3])
    more = "; listing truncated" if truncated else ""
    _print_status(
        "list_objects",
        True,
        f"Can list objects (whole bucket); {len(contents)} shown, "
        f"first keys: {sample}{more}.",
    )
    return True


def _pick_object_key(s3, bucket: str) -> str | None:
    """Return the first object key listed in the bucket, if any."""
    try:
        response = s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
    except Exception:  # noqa: BLE001
        return None
    contents = response.get("Contents", [])
    return contents[0]["Key"] if contents else None


def _check_read_object(s3, bucket: str) -> bool:
    """Verify GetObject/HeadObject permission on a concrete object key."""
    key = _pick_object_key(s3, bucket)
    if not key:
        _print_status(
            "read_object",
            True,
            "No object to read; read check skipped (bucket appears empty).",
        )
        return True

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        _print_status(
            "read_object",
            False,
            f"Cannot read object '{key}' ({error_code}: {exc}).",
        )
        return False
    except Exception as exc:  # noqa: BLE001
        _print_status(
            "read_object",
            False,
            f"Object read failed for '{key}' ({type(exc).__name__}: {exc}).",
        )
        return False

    _print_status("read_object", True, f"Object '{key}' is readable.")
    return True


def _check_cors(s3, bucket: str) -> None:
    """Report whether the bucket has a CORS policy (informational only)."""
    try:
        rules = s3.get_bucket_cors(Bucket=bucket).get("CORSRules", [])
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        print(f"INFO: cors - Bucket '{bucket}' has no CORS configuration "
              f"({error_code}). Cross-origin browser requests may be blocked.")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"INFO: cors - Could not inspect CORS policy "
              f"({type(exc).__name__}: {exc}).")
        return

    allowed_origins = {
        origin
        for rule in rules
        for origin in rule.get("AllowedOrigins", [])
    }
    allowed_methods = {
        method
        for rule in rules
        for method in rule.get("AllowedMethods", [])
    }
    print(f"INFO: cors - Bucket '{bucket}' CORS rules: "
          f"origins={sorted(allowed_origins)}, methods={sorted(allowed_methods)}.")


def _safe_s3_call(s3, operation: str, **kwargs) -> tuple[bool, dict]:
    """Run a read-only S3 call, returning (ok, result-or-error-dict)."""
    try:
        return True, getattr(s3, operation)(**kwargs)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        return False, {"error": error_code}
    except Exception as exc:  # noqa: BLE001
        return False, {"error": f"{type(exc).__name__}: {exc}"}


def _list_level(s3, bucket: str, prefix: str, show_objects: bool = False) -> bool:
    """List one level of the bucket; returns True on success."""
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: Could not list '{prefix}' in bucket '{bucket}': {exc}")
        return False

    folders = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
    for folder in sorted(folders):
        print(f"{folder}")
    if show_objects:
        for obj in response.get("Contents", []):
            print(f"  {obj['Key']} ({obj['Size']} bytes)")

    if response.get("IsTruncated"):
        print("  ... listing truncated")
    return True


def _run_checks(creds: Credentials, bucket: str) -> int:
    """Run the pass/fail access checks for a single bucket."""
    if not _check_credentials_present(creds):
        return 1

    s3 = _make_s3_client(creds)

    results = {
        "credentials_valid": _check_credentials_valid(s3),
        "bucket_reachable": _check_bucket_reachable(s3, bucket),
        "list_objects": _check_list_objects(s3, bucket),
        "read_object": _check_read_object(s3, bucket),
    }
    _check_cors(s3, bucket)

    failed = [name for name in REQUIRED_CHECKS if not results[name]]
    if failed:
        print(f"\nRESULT: FAIL - no full access; failed checks: {', '.join(failed)}.")
        return 1
    print("\nRESULT: PASS - credentials have access to the given bucket.")
    return 0


def _prompt_credentials(current: Credentials | None) -> Credentials:
    """Ask the user for an access key, secret key and endpoint URL."""
    print("\n--- Set Credentials ---")
    print("Enter the credential values; leave a field empty to keep its current value.")
    access_key = _read_line(
        f"Access key [current: {_mask_secret(current.access_key)}]: "
        if current and current.access_key
        else "Access key: "
    )
    secret_key = _read_line(
        f"Secret key [current: {_mask_secret(current.secret_key)}]: "
        if current and current.secret_key
        else "Secret key: "
    )
    endpoint_url = _read_line(
        f"Endpoint URL [current: {current.endpoint_url}]: "
        if current and current.endpoint_url
        else "Endpoint URL: "
    )

    return Credentials(
        access_key=access_key or (current.access_key if current else ""),
        secret_key=secret_key or (current.secret_key if current else ""),
        endpoint_url=endpoint_url or (current.endpoint_url if current else ""),
    )


def _ensure_credentials() -> bool:
    """Make sure session credentials exist, prompting for them if needed."""
    global _session_credentials
    if _session_credentials is not None and _session_credentials.present():
        return True
    _session_credentials = _prompt_credentials(_session_credentials)
    if not _session_credentials.present():
        _print_status(
            "credentials_present",
            False,
            "Set the missing fields via 'Set Credentials' and retry.",
        )
        return False
    return True


def _press_enter_to_return() -> None:
    """Wait for the user before returning to the main menu."""
    _read_line("Press Enter to return to menu")


def _count_objects(s3, bucket: str) -> int:
    """Count all objects in a bucket via pagination; -1 on failure."""
    total = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            total += len(page.get("Contents", []))
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: Could not count objects in '{bucket}': {exc}")
        return -1
    return total


def _full_scan_bucket(s3, bucket: str) -> None:
    """Report reachability, top-level structure and contents for one bucket."""
    print(f"\n-- Bucket: {bucket}")
    ok, _ = _safe_s3_call(s3, "head_bucket", Bucket=bucket)
    if not ok:
        print("  FAIL: bucket not reachable.")
        return
    print("  bucket reachable")
    _list_level(s3, bucket, "", show_objects=True)
    total = _count_objects(s3, bucket)
    if total >= 0:
        print(f"  total objects: {total}")


def _run_full_scan(creds: Credentials) -> int:
    """Show everything the credential pair can do: buckets, structure, contents."""
    print("\n=== FULL SCAN ===")
    print(f"credentials: {creds.masked()}")
    s3 = _make_s3_client(creds)

    ok, response = _safe_s3_call(s3, "list_buckets")
    if not ok:
        print(f"FAIL: credentials_valid - endpoint rejected credentials "
              f"({response.get('error')}).")
        return 1
    buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
    print(f"visible buckets ({len(buckets)}): {', '.join(buckets)}")
    for bucket in buckets:
        _full_scan_bucket(s3, bucket)
    return 0


def _run_access_check(creds: Credentials) -> int:
    """Ask for a user (bucket) name and run the access checks against it."""
    print("\n--- Access Check ---")
    user = _read_line("Enter user name: ")
    if not user:
        print("No user name given; access check aborted.")
        return 1
    return _run_checks(creds, user)


def _print_inspect_row(label: str, ok: bool, value) -> None:
    """Print one read-only introspection row with a short status prefix."""
    status = "OK " if ok else "DENIED"
    if not ok:
        print(f"{status}  {label}: {value.get('error')}")
    elif isinstance(value, str):
        print(f"{status}  {label}: {value}")
    elif isinstance(value, dict):
        value = {k: v for k, v in value.items() if k != "ResponseMetadata"}
        print(f"{status}  {label}: {json.dumps(value, indent=2)}")
    else:
        print(f"{status}  {label}: {json.dumps(value, indent=2)}")


def _inspect_bucket_access(s3, bucket: str) -> None:
    """Show the access model of a bucket: policy, ACL and public access."""
    ok, policy = _safe_s3_call(s3, "get_bucket_policy", Bucket=bucket)
    if ok and policy.get("Policy"):
        try:
            policy_text = json.loads(policy["Policy"])
            _print_inspect_row(f"{bucket}.policy", True, policy_text)
        except (ValueError, TypeError):
            _print_inspect_row(f"{bucket}.policy", True, policy.get("Policy"))
    else:
        _print_inspect_row(f"{bucket}.policy", False, policy)

    ok, acl = _safe_s3_call(s3, "get_bucket_acl", Bucket=bucket)
    if ok:
        _print_inspect_row(f"{bucket}.acl", True, acl)
    else:
        _print_inspect_row(f"{bucket}.acl", False, acl)

    ok, status = _safe_s3_call(s3, "get_bucket_policy_status", Bucket=bucket)
    if ok:
        _print_inspect_row(f"{bucket}.policy_status", True, status)
    else:
        _print_inspect_row(f"{bucket}.policy_status", False, status)

    ok, block = _safe_s3_call(s3, "get_public_access_block", Bucket=bucket)
    if ok:
        _print_inspect_row(f"{bucket}.public_access_block", True, block)
    else:
        _print_inspect_row(f"{bucket}.public_access_block", False, block)


def _run_inspect_access(creds: Credentials) -> int:
    """Ask for a user (bucket) name and show its access model."""
    print("\n--- Inspect Access Policy ---")
    user = _read_line("Enter user name: ")
    if not user:
        print("No user name given; inspection aborted.")
        return 1
    if not _check_credentials_present(creds):
        return 1
    s3 = _make_s3_client(creds)
    print(f"\n=== ACCESS POLICY for user '{user}' (read-only) ===")
    _inspect_bucket_access(s3, user)
    return 0


def _run_menu() -> int:
    """Run the interactive menu loop; every operation returns here."""
    global _session_credentials
    while True:
        print("\n--- S3 Access Tool ---")
        print("1) Set Credentials")
        print("2) Full Scan")
        print("3) Access Check")
        print("4) Inspect Access Policy")
        print("5) Exit")
        print()
        choice = _read_line("Enter your choice: ")
        if not choice and not sys.stdin.isatty():
            return 0

        if choice == "1":
            _session_credentials = _prompt_credentials(_session_credentials)
        elif choice == "2":
            if _ensure_credentials():
                _run_full_scan(_session_credentials)
        elif choice == "3":
            if _ensure_credentials():
                _run_access_check(_session_credentials)
        elif choice == "4":
            if _ensure_credentials():
                _run_inspect_access(_session_credentials)
        elif choice == "5":
            print("Exiting.")
            return 0
        else:
            print("Invalid choice, please try again.")
            continue

        _press_enter_to_return()


def main() -> int:
    """Entry point: run the interactive S3 Access Tool menu."""
    return _run_menu()


if __name__ == "__main__":
    sys.exit(main())
