# ForgeKV Wire Protocol Version 1

Status: Phase 12 implemented format

## 1. Framing

ForgeKV uses a compact binary protocol over TCP. Every integer is unsigned and
encoded in network byte order (big-endian). TCP is treated as a byte stream: a
frame may span any number of reads and one read may contain multiple frames.

Each frame is a 24-byte fixed header followed by exactly `payload_length` bytes:

| Offset | Size | Field | Version 1 meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `FKV1`, bytes `46 4B 56 31` |
| 4 | 1 | version | `01` |
| 5 | 1 | namespace | `01` client, `02` Raft |
| 6 | 1 | message type | namespace-specific value below |
| 7 | 1 | flags | must be zero in version 1 |
| 8 | 8 | request ID | opaque correlation ID |
| 16 | 4 | payload length | bytes after the header |
| 20 | 4 | checksum | IEEE CRC-32 |
| 24 | variable | payload | exactly the declared length |

The CRC-32 uses reflected polynomial `0xEDB88320`, initial state
`0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. It covers header bytes 0–19 followed
by the payload; the checksum field itself is excluded.

The maximum client command is 1,049,664 bytes: a 1 MiB value, 1 KiB key, and
64 bytes of bounded command metadata. The shared frame parser permits a
1,049,856-byte payload and 1,049,880-byte complete frame so the peer namespace
can wrap one maximum client command with bounded Raft metadata. Client semantic
codecs still enforce the smaller operation-specific limits. Implementations
validate length and enum fields before allocating payload storage.

## 2. Message namespaces and types

Client namespace (`01`):

| Type | Name | Direction |
|---:|---|---|
| `01` | PUT | request |
| `02` | GET | request |
| `03` | DELETE | request |
| `04` | PING | request |
| `80` | OK | response |
| `81` | NOT_FOUND | response |
| `82` | ERROR | response |
| `83` | REDIRECT | response |
| `84` | BUSY | response |

Raft namespace (`02`) uses `40` for AppendEntries and its response, `41` for
RequestVote and its response, and `42` for chunked InstallSnapshot traffic.
The payload begins with a fixed peer envelope containing `cluster_id`, source
node ID, destination node ID, and an explicit request/response discriminator,
followed by the bounded message body. Using a type in the wrong namespace is a
protocol error.

## 3. Client payloads

Payload schemas use the same big-endian integer rule. A client ID is 16 opaque
bytes. Mutation clients serialize requests per client ID as specified in
`adr/0005-client-consistency.md`.

```text
PUT:
  0..15   client_id
  16..19  key_length (u32)
  20..23  value_length (u32)
  24..    key bytes, then value bytes

GET:
  0..3    key_length (u32)
  4..     key bytes

DELETE:
  0..15   client_id
  16..19  key_length (u32)
  20..    key bytes

PING:
  0..     opaque echo bytes
```

The frame request ID is the mutation `request_id` for PUT/DELETE and a transport
correlation ID for GET/PING. Key length must be 1–1,024 bytes. Value length may
be 0–1,048,576 bytes. Semantic payload codecs are applied after the framing
parser and never relax the frame limit.

Version 1 response payloads are:

- `OK`: operation-specific bytes; empty for PUT/PING unless PING echoes a
  payload, and one byte (`00` absent, `01` deleted) for DELETE.
- `NOT_FOUND`: empty.
- `ERROR`: `u16 code`, `u16 message_length`, then bounded UTF-8 diagnostic.
- `REDIRECT`: `u16 endpoint_length`, then UTF-8 `host:port`.
- `BUSY`: `u32 retry_after_ms`, where zero means no estimate.

Stable `ERROR` codes are:

| Code | Meaning | Retry policy |
|---:|---|---|
| 1 | malformed or invalid client request | fix the request |
| 2 | replicated operation failed internally | outcome may be unknown; inspect node health |
| 3 | request ID reused with different command bytes | permanent; allocate the next ID correctly |
| 4 | request ID is older than the retained ID | permanent for that request |
| 5 | deduplication retention would exceed 1,024 identities or 64 MiB of command/result bytes | reduce active identity/command retention or wait for a future session-reclamation mechanism |

`BUSY` and `REDIRECT` are transport/leadership outcomes, not deduplication
results. See [`request-deduplication.md`](request-deduplication.md) for the
mutation retry contract.

## 4. Incremental parser behavior

The parser keeps only a fixed header buffer and one validated, bounded payload.
It emits zero or more frames for each supplied chunk. End-of-chunk with a partial
frame is normal; explicit end-of-stream reports `truncated_frame`. Once malformed
input or checksum failure occurs, the parser becomes terminal and the connection
must be closed.

Rejected conditions include invalid magic/version/namespace/type/flags,
namespace/type mismatch, payload beyond the hard maximum, incomplete EOF frame,
and checksum mismatch. Arithmetic uses checked bounded sizes; no allocation is
made from an unvalidated length.

Slow-client protection belongs to the reactor: input bytes and incomplete-frame
age are bounded per connection. The parser itself never waits for more bytes.

## 5. Encoding alternatives

| Property | Fixed-width header | Varints | TLV |
|---|---|---|---|
| CPU/branching | predictable offsets, few branches | branchy decode proportional to width | repeated tag/length dispatch |
| Extensibility | version or reserved flags required | fields still need schema/versioning | strongest unknown-field evolution |
| Simplicity | smallest parser state and easiest bounds proof | canonical encoding and overflow rules required | most parser states and validation |
| Debuggability | direct hex offsets | shifting offsets | self-describing but verbose |
| Bandwidth | 24-byte constant overhead | best for mostly small integers | highest metadata overhead |

Version 1 chooses the fixed-width header. At typical 100 B–4 KiB values, a few
saved header bytes do not justify more branching and malformed-input states.
Extensibility comes from explicit version and namespace fields. A future version
can introduce TLV payloads without changing version 1 parsing.

## 6. Golden example

A client PING with request ID `0102030405060708` and payload `abc` is:

```text
46 4B 56 31 01 01 04 00 01 02 03 04 05 06 07 08
00 00 00 03 B8 4C B9 FF 61 62 63
```

Its checksum `B84CB9FF` covers the first 20 bytes and `61 62 63`.
