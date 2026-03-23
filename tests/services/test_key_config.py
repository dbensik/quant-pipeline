import os
import base64
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from services.crypto.signer import Signer

def test_signer_from_config_ephemeral():
    # When no key is provided, it should generate a new one
    signer = Signer.from_config(None)
    assert signer.private_key is not None
    assert len(signer.get_public_key_id()) > 0

def test_signer_from_config_persistent(monkeypatch):
    # Generate a known key
    priv = ed25519.Ed25519PrivateKey.generate()
    raw_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption()
    )
    b64_key = base64.b64encode(raw_bytes).decode('utf-8')
    
    # Use factory
    signer = Signer.from_config(b64_key)
    
    # Check if public key matches
    expected_pub = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )
    actual_pub = signer.public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )
    assert expected_pub == actual_pub

def test_signer_invalid_key():
    # Invalid length
    with pytest.raises(Exception):
        Signer.from_config("short")
