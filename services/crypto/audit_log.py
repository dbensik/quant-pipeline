import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from services.crypto.hasher import Hasher


class LogRepository(ABC):
    """
    Abstract interface for log persistence.
    """

    @abstractmethod
    def load(self) -> List[Dict]:
        """Load log entries from storage."""
        pass

    @abstractmethod
    def save(self, entries: List[Dict]) -> None:
        """Save log entries to storage."""
        pass


class JsonLogRepository(LogRepository):
    """
    Concrete implementation of LogRepository using a JSON file.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def load(self) -> List[Dict]:
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading audit log: {e}")
        return []

    def save(self, entries: List[Dict]) -> None:
        try:
            with open(self.file_path, "w") as f:
                json.dump(entries, f, indent=2)
        except Exception as e:
            print(f"Error saving audit log: {e}")


class AuditLog:
    """
    Append-only audit log using hash chaining.

    This component emulates a blockchain ledger:
    - Each entry contains the hash of the previous entry ('prev_hash').
    - This creates a cryptographic chain where modifying any past entry would break the chain.
    - This guarantees the immutability and order of the signal history.

    Refactored to follow SRP by delegating persistence to LogRepository.
    """

    def __init__(
        self, repository: Optional[LogRepository] = None, file_path: str = "audit_log.json"
    ):
        """
        Initialize the AuditLog.

        Args:
            repository: An implementation of LogRepository.
            file_path: Legacy path argument. used to create a JsonLogRepository if repository is None.
        """
        if repository:
            self.repository = repository
        else:
            self.repository = JsonLogRepository(file_path)

        self.entries = []
        self.last_hash = "0" * 64
        self._load_log()

    def _load_log(self):
        """Load the log from the repository."""
        self.entries = self.repository.load()
        if self.entries:
            self.last_hash = self.entries[-1]["entry_hash"]
        else:
            self.last_hash = "0" * 64

    def _save_log(self):
        """Save the log to the repository."""
        self.repository.save(self.entries)

    def add_entry(
        self,
        payload_hash: str,
        metadata: str = "",
        signature: Optional[str] = None,
        public_key_id: Optional[str] = None,
    ) -> dict:
        """
        Add a new entry to the immutable log.

        The 'entry_hash' is calculated as:
            SHA256(prev_hash || payload_hash || metadata || timestamp)
        and, for a SIGNED entry (since 2026-10-15),
            SHA256(prev_hash || payload_hash || metadata || timestamp
                   || signature || public_key_id)
        so the seal is part of the chain: changing or stripping it breaks the
        entry's hash. Until then the log held hashes only — the signature went
        to the caller and nothing kept it, so the log could not show that the
        service issued an entry. Entries written before stay valid under the
        old form.
        """
        timestamp = datetime.utcnow().isoformat()

        content_to_hash = _content(
            self.last_hash, payload_hash, metadata, timestamp, signature, public_key_id
        )
        entry_hash = Hasher.hash_str(content_to_hash)

        entry = {
            "index": len(self.entries),
            "prev_hash": self.last_hash,
            "payload_hash": payload_hash,
            "metadata": metadata,
            "timestamp": timestamp,
            "entry_hash": entry_hash,
        }
        if signature is not None:
            entry["signature"] = signature
            entry["public_key_id"] = public_key_id

        self.entries.append(entry)
        self.last_hash = entry_hash

        self._save_log()  # Persist immediately

        return entry

    def get_log(self) -> list:
        return self.entries

    def verify_integrity(self) -> bool:
        """
        Verify the hash chain integrity of the entire log.
        """
        prev_hash = "0" * 64
        for entry in self.entries:
            if entry["prev_hash"] != prev_hash:
                return False

            content_to_hash = _content(
                prev_hash,
                entry["payload_hash"],
                entry["metadata"],
                entry["timestamp"],
                entry.get("signature"),
                entry.get("public_key_id"),
            )
            if Hasher.hash_str(content_to_hash) != entry["entry_hash"]:
                return False

            prev_hash = entry["entry_hash"]
        return True

    def verify_signatures(self, public_key_pem: str) -> Tuple[int, int, List[int]]:
        """
        Check every signed entry's signature against a TRUSTED public key — the
        published one, never a key taken from the log, which a forger could
        supply. Returns (signed, unsigned, indexes that fail): an entry fails
        when its key id is not that key's or its signature does not verify
        over its payload hash. Entries from before 2026-10-15 are unsigned.
        """
        from services.crypto.signer import Signer

        trusted_id = Signer.public_key_id_of(public_key_pem)
        signed, unsigned, failures = 0, 0, []
        for entry in self.entries:
            if "signature" not in entry:
                unsigned += 1
                continue
            signed += 1
            ok = entry.get("public_key_id") == trusted_id and Signer.verify(
                public_key_pem, entry["payload_hash"].encode("utf-8"), entry["signature"]
            )
            if not ok:
                failures.append(entry["index"])
        return signed, unsigned, failures


def _content(
    prev_hash: str,
    payload_hash: str,
    metadata: str,
    timestamp: str,
    signature: Optional[str],
    public_key_id: Optional[str],
) -> str:
    """What an entry's hash covers; a signed entry adds its seal."""
    base = f"{prev_hash}{payload_hash}{metadata}{timestamp}"
    if signature is None:
        return base
    return f"{base}{signature}{public_key_id}"
