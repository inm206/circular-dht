# Circular DHT — peer-to-peer distributed hash table

A circular DHT implemented from scratch over raw sockets in Python
(~320 lines, no libraries beyond the standard library). Each peer is a
process that knows its two successors, and the ring self-heals when peers
leave or die.

## What it does

- **Ring maintenance over UDP** — each peer pings its successors on port
  `50000 + peer_id` and responds to its predecessors, so every node
  continuously verifies its neighbours are alive.
- **File requests over TCP** — a file number is hashed (`hash % 256`) to
  decide the responsible peer; requests are forwarded around the ring until
  the owner answers the requester directly.
- **Graceful departure** — a leaving peer notifies both predecessors and
  both successors so they can re-link the ring (`gracefulExit`).
- **Crash recovery** — when pings to a successor stop being answered, the
  peer queries the remaining ring to rebuild its successor pointers
  (`killedPeer`).

## Run

Each peer takes its own id and its first and second successors:

```bash
python3 cdht.py 1 3 4 &
python3 cdht.py 3 4 5 &
python3 cdht.py 4 5 8 &
python3 cdht.py 5 8 10 &
python3 cdht.py 8 10 12 &
# ... close the ring back to peer 1
```

Then interact with a peer via stdin: `request 2012` asks the ring for file
2012, and `quit` leaves gracefully.

## Background

Written for UNSW COMP9331 Computer Networks & Applications (2018).
