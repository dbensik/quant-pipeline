import hashlib
import json
from typing import Any

class Hasher:
    """
    Utilities for consistent hashing (SHA256).
    
    This class provides a unified interface for hashing different data types.
    SHA256 is used as the standard hashing algorithm for this pipeline due to its
    strong collision resistance and wide industry adoption.
    """

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """
        Return hex digest of SHA256 hash of raw bytes.
        """
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def hash_str(data: str) -> str:
        """
        Return hex digest of SHA256 hash of a standard UTF-8 string.
        """
        return Hasher.hash_bytes(data.encode('utf-8'))

    @staticmethod
    def hash_dict(data: dict) -> str:
        """
        Return hex digest of SHA256 hash of a dictionary (canonical JSON).
        
        CRITICAL: We must ensure that the dictionary is serialized deterministically.
        - sort_keys=True: Ensures keys are always in alphabetical order.
        - separators=(',', ':'): Removes unnecessary whitespace which might vary.
        
        This ensures that {'a': 1, 'b': 2} and {'b': 2, 'a': 1} produce the EXACT same hash.
        """
        # sort_keys=True ensures canonical representation
        serialized = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return Hasher.hash_str(serialized)
