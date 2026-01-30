"""Utility functions"""
import base64
import hashlib
from typing import Optional

try:
    from Crypto.Cipher import ARC4
    from Crypto.Cipher import ARC4
    from Crypto.Hash import MD5
    from Crypto import Random
except ImportError:
    raise ImportError(
        "pycryptodome is required. Install it with: pip install pycryptodome"
    )


def encrypt_rc4_cryptojs(plaintext, password):
    """
    Simulate CryptoJS.RC4.encrypt behavior
    Supports OpenSSL-compatible Salted__ format
    """
    # 1. Generate random 8-byte salt
    salt = Random.get_random_bytes(8)
    
    # 2. Simulate OpenSSL's key derivation logic (EVP_BytesToKey)
    password_bytes = password.encode('utf-8')
    derived_bytes = b""
    last_hash = b""
    while len(derived_bytes) < 32:
        last_hash = MD5.new(last_hash + password_bytes + salt).digest()
        derived_bytes += last_hash
    key = derived_bytes[:32]
    
    # 3. Perform RC4 encryption
    cipher = ARC4.new(key)
    ciphertext = cipher.encrypt(plaintext.encode('utf-8'))
    
    # 4. Concatenate format: "Salted__" + salt + ciphertext and convert to Base64
    final_payload = b'Salted__' + salt + ciphertext
    return base64.b64encode(final_payload).decode('utf-8')


def rc4_encrypt(text: str, key: str) -> str:
    """
    Encrypt text using RC4 algorithm
    
    Args:
        text: Text to encrypt
        key: Encryption key
        
    Returns:
        Base64 encoded encrypted result
    """
    return encrypt_rc4_cryptojs(text, key)


def gen_sign(email: str, password: str, stime: int, key: str) -> str:
    """
    Generate signature
    
    Args:
        email: Email
        password: Encrypted password
        stime: Timestamp
        key: Signature key
        
    Returns:
        SHA1 signature (uppercase)
    """
    raw = f"{email}{password}{stime}{key}"
    return hashlib.sha1(raw.encode()).hexdigest().upper()


def build_filter(
    skip: int = 0,
    limit: int = 20,
    where: Optional[dict] = None,
    fields: Optional[dict] = None,
    order: Optional[str] = None,
    noSchema: Optional[int] = 1,
) -> dict:
    """
    Build query filter
    
    Args:
        skip: Number of records to skip
        limit: Limit on number of results
        where: Query conditions
        fields: Field filter
        order: Sort order
        
    Returns:
        Filter dictionary
    """
    filter_dict = {
        "skip": skip,
        "limit": limit,
    }
    
    if where:
        filter_dict["where"] = where
    
    if fields:
        filter_dict["fields"] = fields
    
    if order:
        filter_dict["order"] = order
    filter_dict["noSchema"] = 1
    
    return filter_dict
