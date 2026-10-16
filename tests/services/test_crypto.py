import json

from services.crypto.audit_log import AuditLog
from services.crypto.hasher import Hasher
from services.crypto.signer import Signer


# --- Hasher Tests ---
def test_hasher_str():
    input_str = "hello"
    # Matches `echo -n "hello" | shasum -a 256`
    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert Hasher.hash_str(input_str) == expected


def test_hasher_dict_canonical():
    d1 = {"a": 1, "b": 2}
    d2 = {"b": 2, "a": 1}  # Different order
    # Should produce identical hashes
    assert Hasher.hash_dict(d1) == Hasher.hash_dict(d2)


# --- Signer Tests ---
def test_signer_sign_verify():
    signer = Signer()
    message = b"important data"
    signature = signer.sign(message)

    # Verify using the instance method logic (or manually)
    # MUST pass the PEM string, not the object
    # ARG ORDER: pem, message, signature
    assert Signer.verify(signer.get_public_key_pem(), message, signature) is True


def test_signer_verify_failure():
    signer = Signer()
    message = b"data"
    signature = signer.sign(message)

    # Tamper with message
    assert Signer.verify(signer.get_public_key_pem(), b"tampered", signature) is False


# --- Audit Log Tests ---
def test_audit_log_chaining(tmp_path):
    # Use a temporary file for the audit log
    log_file = tmp_path / "test_audit_log.json"
    audit = AuditLog(file_path=str(log_file))

    # Add Entry 1
    e1 = audit.add_entry(payload_hash="hash1", metadata="meta1")
    assert e1["index"] == 0
    assert e1["prev_hash"] == "0" * 64

    # Add Entry 2
    e2 = audit.add_entry(payload_hash="hash2", metadata="meta2")
    assert e2["index"] == 1
    # Verify chaining: e2's prev_hash must match e1's entry_hash
    assert e2["prev_hash"] == e1["entry_hash"]

    # Verify Integrity
    assert audit.verify_integrity() is True


def test_audit_log_tamper_detect(tmp_path):
    log_file = tmp_path / "test_audit_log_tamper.json"
    audit = AuditLog(file_path=str(log_file))
    audit.add_entry("hash1")
    audit.add_entry("hash2")

    # Manually tamper with the file
    with open(log_file, "r+") as f:
        data = json.load(f)
        # Modify the first entry's payload hash without updating the chain
        # The log file root is a list of entries
        data[0]["payload_hash"] = "EVIL_HASH"
        f.seek(0)
        json.dump(data, f)
        f.truncate()

    # Reload and verify
    audit_tampered = AuditLog(file_path=str(log_file))
    assert audit_tampered.verify_integrity() is False


# --- Signed audit entries (2026-10-15): the seal is in the entry and its hash ---
def test_a_signed_entry_keeps_its_seal_in_the_chain(tmp_path):
    signer = Signer()
    audit = AuditLog(file_path=str(tmp_path / "signed.json"))
    audit.add_entry("legacy")  # an entry from before signing
    sig = signer.sign(b"hash2")
    e = audit.add_entry("hash2", "meta", signature=sig, public_key_id=signer.get_public_key_id())
    assert e["signature"] == sig and e["public_key_id"] == signer.get_public_key_id()
    assert AuditLog(file_path=str(tmp_path / "signed.json")).verify_integrity() is True


def _tamper(path, change):
    data = json.loads(path.read_text())
    change(data[-1])
    path.write_text(json.dumps(data))


def test_changing_or_stripping_a_seal_breaks_the_chain(tmp_path):
    signer = Signer()
    for change in (
        lambda e: e.update(signature=Signer().sign(b"hash1")),
        lambda e: e.pop("signature") and e.pop("public_key_id"),
    ):
        path = tmp_path / f"t{id(change)}.json"
        audit = AuditLog(file_path=str(path))
        audit.add_entry(
            "hash1", signature=signer.sign(b"hash1"), public_key_id=signer.get_public_key_id()
        )
        _tamper(path, change)
        assert AuditLog(file_path=str(path)).verify_integrity() is False


def test_signatures_are_checked_against_the_trusted_key(tmp_path):
    signer, forger = Signer(), Signer()
    audit = AuditLog(file_path=str(tmp_path / "keys.json"))
    audit.add_entry("old")
    audit.add_entry(
        "good", signature=signer.sign(b"good"), public_key_id=signer.get_public_key_id()
    )
    # A forger's entry is chain-valid — it is appended properly — but not the service's.
    audit.add_entry(
        "forged", signature=forger.sign(b"forged"), public_key_id=forger.get_public_key_id()
    )
    assert audit.verify_integrity() is True
    signed, unsigned, bad = audit.verify_signatures(signer.get_public_key_pem())
    assert (signed, unsigned, bad) == (2, 1, [2])


def test_a_claimed_key_id_is_not_trusted_on_its_own(tmp_path):
    signer, forger = Signer(), Signer()
    audit = AuditLog(file_path=str(tmp_path / "claim.json"))
    audit.add_entry("x", signature=forger.sign(b"x"), public_key_id=signer.get_public_key_id())
    assert audit.verify_signatures(signer.get_public_key_pem())[2] == [0]


def test_a_key_id_can_be_derived_from_a_published_pem():
    signer = Signer()
    assert Signer.public_key_id_of(signer.get_public_key_pem()) == signer.get_public_key_id()
