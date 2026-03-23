import os
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime
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
    def __init__(self, repository: Optional[LogRepository] = None, file_path: str = "audit_log.json"):
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

    def add_entry(self, payload_hash: str, metadata: str = "") -> dict:
        """
        Add a new entry to the immutable log.
        
        The 'entry_hash' is calculated as:
            SHA256(prev_hash || payload_hash || metadata || timestamp)
        """
        timestamp = datetime.utcnow().isoformat()
        
        # Construct content string to hash (simple concatenation for MVP)
        content_to_hash = f"{self.last_hash}{payload_hash}{metadata}{timestamp}"
        entry_hash = Hasher.hash_str(content_to_hash)

        entry = {
            "index": len(self.entries),
            "prev_hash": self.last_hash,
            "payload_hash": payload_hash,
            "metadata": metadata,
            "timestamp": timestamp,
            "entry_hash": entry_hash
        }
        
        self.entries.append(entry)
        self.last_hash = entry_hash
        
        self._save_log() # Persist immediately
        
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
            
            content_to_hash = f"{prev_hash}{entry['payload_hash']}{entry['metadata']}{entry['timestamp']}"
            if Hasher.hash_str(content_to_hash) != entry["entry_hash"]:
                return False
            
            prev_hash = entry["entry_hash"]
        return True
