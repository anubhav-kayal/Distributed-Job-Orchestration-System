import hashlib
import secrets

PREFIX = "djos_"


def generate_api_key() -> str:
    """A new API key: 32 random bytes (256 bits), URL-safe."""
    return PREFIX + secrets.token_urlsafe(32)


def hash_api_key(key: str) -> str:
    # Only the hash is stored, so a database leak doesn't leak working keys. SHA-256 is
    # enough because the key itself has 256 bits of entropy; slow hashes like bcrypt
    # exist to stop brute-forcing low-entropy human passwords.
    return hashlib.sha256(key.encode()).hexdigest()
