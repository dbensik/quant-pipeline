import pytest
import os
import json
from services.crypto.signer import Signer
from services.crypto.hasher import Hasher
from services.crypto.audit_log import AuditLog

# --- Hasher Tests ---
def test_hasher_str():
    input_str = "hello"
    # Matches `echo -n "hello" | shasum -a 256`
    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert Hasher.hash_str(input_str) == expected

def test_hasher_dict_canonical():
    d1 = {"a": 1, "b": 2}
    d2 = {"b": 2, "a": 1} # Different order
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
