import json
import sys

from services.crypto.hasher import Hasher


def verify_integration():
    """
    Runs a full integration verification of the 3-layer architecture.

    Steps:
    1. Query GraphQL: Fetches a signal for 'BTC-USD' (the registry symbol; until
       2026-10-14 'BTC', which Yahoo serves as the Grayscale Bitcoin Mini Trust
       ETF) via the gateway.
    2. Local Verify: Re-calculates the SHA256 hash of the payload and compares it
       to the 'featuresHash' returned by the server, then VERIFIES the signature
       over that hash against the published public key
       (services/crypto/signing_public_key.pem) and checks the key id matches.
       Until 2026-10-15 it only checked a signature was present.
    3. Audit Check: Loads the 'audit_log.json' file, verifies the hash chain,
       and verifies every signed entry's signature against the same key.

    Exits 1 on any failure. It used to print FAIL and exit 0, so
    `./run_pipeline.sh verify` passed whatever it found.
    """
    failures = []
    print("--- 1. Querying GraphQL Gateway ---")
    query = """
        query {
            signal(symbol: "BTC-USD", asOf: "2023-01-01") {
                value
                confidence
                signature
                featuresHash
                publicKeyId
                payloadJson
            }
        }
    """

    # Execute query against the running GraphQL Gateway (HTTP)
    # This ensures we test the full stack (Gateway -> Async Resolver -> gRPC -> Service)
    import requests

    from services.config import ServiceConfig

    url = ServiceConfig.get_graphql_url()
    try:
        response = requests.post(url, json={"query": query}, timeout=30)
        response.raise_for_status()
        result_json = response.json()

        if "errors" in result_json:
            print("GraphQL Errors:", result_json["errors"])
            sys.exit(1)

        data = result_json["data"]["signal"]
        print("GraphQL Response:", json.dumps(data, indent=2))

    except Exception as e:
        print(f"FAIL: Could not connect to GraphQL Gateway at {url}")
        print(f"Error: {e}")
        sys.exit(1)

    print("\n--- 2. Verifying Signature Locally ---")
    # Extract fields needed for verification
    signature = data["signature"]
    payload_json = data["payloadJson"]
    features_hash = data["featuresHash"]

    # Re-calculate hash from the raw JSON payload
    # This ensures that 'featuresHash' wasn't spoofed; it must match the data content.
    calculated_hash = Hasher.hash_bytes(payload_json.encode("utf-8"))
    print(f"Calculated Hash: {calculated_hash}")
    print(f"Received Hash:   {features_hash}")

    if calculated_hash != features_hash:
        print("FAIL: Hash mismatch!")
        failures.append("hash")
    else:
        print("PASS: Hash matches.")

    from services.config import ServiceConfig
    from services.crypto.signer import Signer

    try:
        public_key_pem = open(ServiceConfig.PUBLIC_KEY_PATH).read()
    except OSError:
        public_key_pem = None
        print(f"FAIL: no published public key at {ServiceConfig.PUBLIC_KEY_PATH}")
        failures.append("public key")
    if public_key_pem:
        trusted_id = Signer.public_key_id_of(public_key_pem)
        if data["publicKeyId"] != trusted_id:
            print(
                f"FAIL: signed by key {data['publicKeyId']}, published key is {trusted_id} "
                "(an ephemeral key, or QUANT_PRIVATE_KEY changed)"
            )
            failures.append("key id")
        elif Signer.verify(public_key_pem, features_hash.encode("utf-8"), signature):
            print(f"PASS: Signature verifies against the published key {trusted_id}.")
        else:
            print("FAIL: Signature does not verify.")
            failures.append("signature")

    print("\n--- 3. Verifying Audit Log ---")
    # Check if log file exists
    import os

    from services.crypto.audit_log import AuditLog

    if not os.path.exists(ServiceConfig.AUDIT_LOG_PATH):
        print(f"FAIL: {ServiceConfig.AUDIT_LOG_PATH} not found.")
        failures.append("audit log")
    else:
        # Load the log and traverse the chain
        audit_log = AuditLog(file_path=ServiceConfig.AUDIT_LOG_PATH)
        entries = audit_log.get_log()
        print(f"Log contains {len(entries)} entries.")

        if audit_log.verify_integrity():
            print("PASS: Audit Log Integrity Verified (Chain is valid).")
        else:
            print("FAIL: Audit Log Integrity Check Failed!")
            failures.append("chain")
        if public_key_pem:
            signed, unsigned, bad = audit_log.verify_signatures(public_key_pem)
            if bad:
                print(f"FAIL: {len(bad)} signed entr(ies) do not verify: {bad[:10]}")
                failures.append("log signatures")
            else:
                print(
                    f"PASS: {signed} signed entr(ies) verify against the published key "
                    f"({unsigned} from before entries were signed)."
                )

    if failures:
        print(f"\nVERIFY FAILED: {', '.join(failures)}")
        sys.exit(1)
    print("\nVERIFY PASSED")


if __name__ == "__main__":
    verify_integration()
