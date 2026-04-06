# Updating LimeWire Encryption Constants

magsync auto-extracts LimeWire encryption constants on first download. If auto-extraction fails, this document describes how to manually re-extract them.

## Constants Used by magsync

| Constant | Purpose |
|----------|---------|
| `sharing_salt_b64` | PBKDF2 salt for deriving passphrase wrapping key |
| `sharing_iv_b64` | IV for AES-GCM encryption of sharing passphrase |
| `file_iv_b64` | Static 11-byte IV for AES-CTR file content decryption |
| `file_name_iv_b64` | IV for AES-GCM decryption of file names |
| `file_sha1_iv_b64` | IV for AES-GCM decryption of file SHA1 hashes |
| `preview_iv_b64` | IV for AES-CTR decryption of preview images |
| `pbkdf2_iterations` | Iteration count for PBKDF2 key derivation (default: 100000) |

These are **not shipped with the repository**. They are auto-extracted from LimeWire's JS bundles on first download and saved to `~/.magsync/config.toml` under `[limewire]`. If constants become stale (LimeWire deploys new bundles), magsync self-heals by re-extracting automatically.

## Extraction Process

All constants are embedded in LimeWire's client-side JS bundles. The process below uses a browser's DevTools console but can also be done with curl + grep.

### Step 1: Find the constants chunk

Visit any LimeWire share link (e.g., `https://limewire.com/d/bjAa5#jLqFy2nOr0`).

Open DevTools → Network tab. Look for JS chunks loaded from `/build/chunks/`. The constants live in a chunk named something like `get-sharing-bucket-expiration-label-*.js`. The exact hash suffix changes per deploy.

To find it programmatically, search the page's `<link rel="modulepreload">` tags or the `__manifest` endpoint:

```
GET https://limewire.com/__manifest?paths=%2Fd%2FbjAa5&version=<current_version>
```

The route `routes/__root/d/$id` imports the chunk chain. The constants chunk is imported by `google-adsense-ad-*.js`, which is imported by the `_id-*.js` route chunk.

### Step 2: Extract sharing passphrase encryption details

In the constants chunk, search for `ivBase64` and `saltBase64`. They appear in an object like:

```javascript
F = {
  ivBase64: "2VZyNE4xkZ9oo6/B",
  saltBase64: "wvsoOvbI854RHQMiSiPmnw=="
}
```

This object is assigned to `sharingPassphraseEncryptionDetails` in a larger config:

```javascript
Y = {
  accountSubscription: M,
  staticContentItemEncryptionIvs: _,
  sharingPassphraseEncryptionDetails: F,
  creditsToUsdcFactor: W
}
```

### Step 3: Extract static file encryption IVs

In the same chunk, search for `mainFileBase64`. The IVs are in an object like:

```javascript
_ = {
  mainFileNameBase64: "EtrUFVLIRAW8aUCd",
  mainFileBase64: "C8aZG384/qPpBzg=",
  mainFileSha1Base64: "6Q+YlJkg8RFR/FHN",
  previewFileBase64: "i3iv8Nv2xEje9VE="
}
```

These same IVs also appear in the service worker (`/build/workers/service-worker.js`), which can be an easier target to search:

```bash
curl -s "https://limewire.com/build/workers/service-worker.js" | \
  grep -oP 'mainFileBase64:"[^"]+"'
```

### Step 4: Verify PBKDF2 iteration count

Search for `iterations:` near `PBKDF2` in either the constants chunk or `file-sharing-provider-*.js`:

```javascript
iterations: 1e5  // = 100000
```

### Quick Verification Script

After extracting new constants, verify them against a known working share link:

```python
import httpx, json, base64, re
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.keywrap import aes_key_unwrap
from cryptography.hazmat.primitives.asymmetric import ec
from urllib.parse import urlparse

# --- UPDATE THESE IF CHANGED ---
SHARING_SALT_B64 = "wvsoOvbI854RHQMiSiPmnw=="
FILE_IV_B64 = "C8aZG384/qPpBzg="
PBKDF2_ITERATIONS = 100000
# --------------------------------

TEST_URL = "https://limewire.com/d/bjAa5#jLqFy2nOr0"

sharing_id = urlparse(TEST_URL).path.split("/")[-1]
fragment = urlparse(TEST_URL).fragment

client = httpx.Client(follow_redirects=True, timeout=60.0)
html = client.get(f"https://limewire.com/d/{sharing_id}").text
jwt_token = client.cookies.get("production_access_token")
csrf = json.loads(base64.b64decode(jwt_token.split('.')[1] + '==='))["csrfToken"]

UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
def ssr(field):
    m = re.search(r'\\"' + re.escape(field) + r'\\",\\"([^"\\]+)\\"', html)
    return m.group(1) if m else None

bucket_id = ssr('id')
content_id = re.search(r'contentItemIds.*?(' + UUID + ')', html).group(1)
wrapped_pk = ssr('passphraseWrappedPrivateKey')
eph_pub = ssr('ephemeralPublicKey')

kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                  salt=base64.b64decode(SHARING_SALT_B64), iterations=PBKDF2_ITERATIONS)
wk = kdf.derive(fragment.encode())
raw_pk = aes_key_unwrap(wk, base64.b64decode(wrapped_pk))
pk = ec.derive_private_key(int.from_bytes(raw_pk, 'big'), ec.SECP256R1())
aes_key = pk.exchange(ec.ECDH(),
    ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), base64.b64decode(eph_pub)))

r = client.post(f"https://api.limewire.com/sharing/download/{bucket_id}",
    headers={"X-CSRF-Token": csrf, "Authorization": f"Bearer {jwt_token}",
             "Content-Type": "application/json"},
    json={"contentItems": [{"id": content_id}]})
s3_url = r.json()["contentItems"][0]["downloadUrl"]

enc = client.get(s3_url, headers={"Range": "bytes=0-1023"}).content
iv = base64.b64decode(FILE_IV_B64)
nonce = bytearray(16)
nonce[:len(iv)] = iv
dec = Cipher(algorithms.AES(aes_key), modes.CTR(bytes(nonce))).decryptor().update(enc)

assert dec[:4] == b'%PDF', f"FAILED: got {dec[:4]}"
print("SUCCESS: constants are valid")
```

## Source Locations Summary

| Constant | JS Bundle | Search Pattern |
|----------|-----------|----------------|
| `saltBase64`, `ivBase64` | `get-sharing-bucket-expiration-label-*.js` | `saltBase64` |
| File IVs (`mainFileBase64`, etc.) | Same chunk, OR `service-worker.js` | `mainFileBase64` |
| PBKDF2 iterations | `file-sharing-provider-*.js` | `iterations:1e5` |
| AES-CTR counter length | `service-worker.js` | `length:40` |
| AES-CTR nonce size | `service-worker.js` | `byteLength!==11` (IV must be 11 bytes) |
