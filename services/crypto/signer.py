import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

class Signer:
    """
    Handles Ed25519 key generation, signing, and verification.
    
    Ed25519 is selected for:
    1. High performance (fast signing/verification).
    2. Small key/signature sizes (32 bytes public key, 64 bytes signature).
    3. Deterministic signatures (no random nonce required during signing).
    """

    def __init__(self, private_key=None):
        """
        Initialize the Signer.
        
        Args:
            private_key: Optional existing Ed25519PrivateKey. 
                         If None, a fresh ephemeral keypair is generated for this session.
                         In production, you would load this from a secure HSM or Vault.
        """
        if private_key:
            self.private_key = private_key
        else:
            # Generate a new private key for this instance
            self.private_key = ed25519.Ed25519PrivateKey.generate()
            
        # Derive the public key immediately
        self.public_key = self.private_key.public_key()
        
    @classmethod
    def from_config(cls, b64_key: str = None):
        """
        Factory to create a Signer from a base64 encoded private key string.
        """
        if b64_key:
            try:
                # Decode base64 to 32 bytes
                key_bytes = base64.b64decode(b64_key)
                if len(key_bytes) != 32:
                    raise ValueError(f"Invalid Ed25519 private key length: {len(key_bytes)}")
                
                private_key = ed25519.Ed25519PrivateKey.from_private_bytes(key_bytes)
                return cls(private_key=private_key)
            except Exception as e:
                print(f"Error loading private key from config: {e}")
                raise e
        return cls() # Ephemeral fallback

    def sign(self, message: bytes) -> str:
        """
        Sign a bytes message using the private key.
        
        Args:
            message (bytes): The raw data to sign.
            
        Returns:
            str: The base64-encoded signature string.
        """
        signature = self.private_key.sign(message)
        return base64.b64encode(signature).decode('utf-8')

    def get_public_key_pem(self) -> str:
        """
        Return the public key in standard PEM format.
        Useful for sharing with clients who need to verify signatures.
        """
        pem = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem.decode('utf-8')

    def get_public_key_id(self) -> str:
        """
        Generate a short, unique identifier for this public key.
        This ID is attached to signals so clients know WHICH key signed the data.
        
        Returns:
             str: First 16 characters of the base64-encoded raw public key bytes.
        """
        pub_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        return base64.b64encode(pub_bytes).decode('utf-8')[:16]

    @staticmethod
    def verify(public_key_pem: str, message: bytes, signature_b64: str) -> bool:
        """
        Static utility to verify a signature against a public key.
        
        Args:
            public_key_pem (str): The signer's public key in PEM format.
            message (bytes): The original data that was signed.
            signature_b64 (str): The base64-encoded signature to verify.
            
        Returns:
            bool: True if signature is valid, False otherwise.
        """
        try:
            public_key = serialization.load_pem_public_key(public_key_pem.encode('utf-8'))
            signature = base64.b64decode(signature_b64)
            public_key.verify(signature, message)
            return True
        except Exception:
            # Any error (bad encoding, invalid signature) results in False
            return False
