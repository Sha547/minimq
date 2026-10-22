# minimq

A tiny publish/subscribe message broker over TCP, built with only Node's
built-in modules. A learning project in the same style as
[miniredis](https://github.com/Sha547/miniredis).

```bash
node broker.js       # terminal 1: listens on 127.0.0.1:7000
node client.js       # terminals 2 and 3: interactive prompts
node test.js         # automated tests
```

Try it: in one client `SUB news`; in another `PUB news hello there`. The first
client prints `MSG news hello there`.

## Protocol

Newline-delimited text. Command names are case-insensitive.

| Command | Reply |
|---|---|
| `SUB topic` | `OK` |
| `UNSUB topic` | `OK` |
| `PUB topic message…` | `(integer) N`, the number of subscribers it reached |
| `TOPICS` | space-separated topic names, or `(empty list)` |
| `PING` | `PONG` |

Subscribers are sent `MSG topic message…` whenever someone publishes.

## Concepts

- **Pub/sub decoupling.** Publishers name a topic, not a recipient. The broker
  keeps `topic → Set<socket>` and fans each message out.
- **Server push.** Unlike request/response, `MSG` lines arrive unprompted, so
  the client redraws its prompt around them.
- **Fire-and-forget delivery.** Nothing is stored: a late subscriber misses
  earlier messages. Durable brokers (Kafka, RabbitMQ) add logs and acks.
- **Cleanup on disconnect.** Closed sockets are removed from every topic so
  they neither leak memory nor receive writes.
- **Framing.** TCP is a byte stream, so each connection buffers and splits on
  `\n`.

## Limits

No auth, no persistence, no wildcards, messages can't contain newlines.
