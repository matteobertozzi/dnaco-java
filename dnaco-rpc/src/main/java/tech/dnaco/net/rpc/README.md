## RPC Header packets are composed of:
- *Packet Type*: 2bit (REQUEST, RESPONSE, EVENT, CONTROL)
- *Flags*: 3bit (specific to packet type)
- *Packet Id length*: 3bit (1 + (0-7)) max 8bytes int
```
+----+-----+-----+ +----------+ +---------+ +-----------+
| 11 | --- | 111 | | Trace Id | | Span Id | | Packet Id |
+----+-----+-----+ +----------+ +---------+ +-----------+
0    2     5     8          136         200  (1-8 bytes)
```

### RPC Request Header packets are composed of:
 - *Send Result To*: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
 - *Request Id Length*: 7bit (1 + (0-127)) max 128bytes string
 - *Result Id Length*: 7bit (1 + (0-127)) max 127bytes string. used only when Send Result To is not CALLER.
```
+----+---------+--------+ +-------------+ +-------------+
| 11 | 1111111 | 111111 | | Request Id  | |  Result Id  |
+----+---------+--------+ +-------------+ +-------------+
0    2         9       16  (1-128 bytes)   (1-128 bytes)
```


### RPC Response Header Packets are composed of:
 - *Operation Status*: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
 - *Queue Time length*: 3bit (1 + (0-7)) max 8bytes int
 - *Exec Time length*: 3bit (1 + (0-7)) max 8bytes int
```
+----+-----+-----+ +---------------+ +---------------+
| 11 | 111 | 111 | | Queue Time ns | | Exec Time ns  |
+----+-----+-----+ +---------------+ +---------------+
0    2     5     8    (1-8 bytes)       (1-8 bytes)
```


### RPC Event Header Packets are composed of:
 - A Variable-Length Integer containing the length of the EventId. 1byte allows max 240 eventId length (https://sqlite.org/src4/doc/trunk/www/varint.wiki)
```
+----------+ +----------+
| 11111111 | | Event Id |
+----------+ +----------+
0          8
```