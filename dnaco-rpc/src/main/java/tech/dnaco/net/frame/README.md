## FRAME
Frames are in the form of | length (u32) | ...data... |
- the first 5bit are used to identify the protocol version (0-31).
- the following 27bit are used to identify the packet length (max 128M)
```
+-------+--------------------------------------+
| 11111 | 111 | 11111111 | 11111111 | 11111111 |
+-------+--------------------------------------+
0 rev.  5             data length             32
```

### Encryption/Signature
If frame are sent over an insecure transport encryption can be applied.
The first 8bit are used to describe the algorithm used. The assumption here is that we used something like RSA + AES, so we have the aes key encrypted with the RSA key and the signature.
```
+----------+ +----------------+ +-----------+
| 11111111 | | encryption key | | signature |
+----------+ +----------------+ +-----------+
0          8      (N bytes)       (N bytes)
```
