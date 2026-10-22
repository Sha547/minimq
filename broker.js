'use strict';

const net = require('node:net');

/*
 * broker.js — a tiny publish/subscribe message broker.
 *
 * Publish/subscribe decouples senders from receivers: a publisher sends a
 * message to a named TOPIC without knowing who (if anyone) is listening, and
 * the broker pushes it to every client currently subscribed to that topic.
 * Unlike a request/response server, the broker also sends data a client never
 * asked for in that moment, so replies and pushed messages share one socket.
 *
 * Wire protocol (newline-delimited text, like miniredis):
 *   SUB topic            -> OK
 *   UNSUB topic          -> OK
 *   PUB topic message... -> (integer) N     N = subscribers it was delivered to
 *   TOPICS               -> topic list, "(empty list)" if none
 *   PING                 -> PONG
 * Pushed to subscribers:  MSG topic message...
 *
 * Messages are NOT stored. A client that subscribes after a PUB never sees it
 * (this is "at-most-once, fire-and-forget"; Kafka/RabbitMQ add durability).
 */

function createBroker({ port = 7000, host = '127.0.0.1' } = {}) {
  // topic name -> Set of sockets subscribed to it
  const topics = new Map();

  function subscribe(socket, topic) {
    if (!topics.has(topic)) topics.set(topic, new Set());
    topics.get(topic).add(socket);
  }

  function unsubscribe(socket, topic) {
    const subs = topics.get(topic);
    if (!subs) return;
    subs.delete(socket);
    if (subs.size === 0) topics.delete(topic); // don't leak empty topics
  }

  function publish(topic, message) {
    const subs = topics.get(topic);
    if (!subs) return 0;
    for (const socket of subs) socket.write(`MSG ${topic} ${message}\n`);
    return subs.size;
  }

  function handle(socket, line) {
    // Split off the command and topic; everything after is the message, so
    // messages may contain spaces.
    const [, cmd = '', topic = '', message = ''] = line.trim().match(/^(\S+)(?:\s+(\S+))?(?:\s+([\s\S]*))?$/) ?? [];
    switch (cmd.toUpperCase()) {
      case 'PING':
        return 'PONG';
      case 'SUB':
        if (!topic) return '(error) ERR SUB needs a topic';
        subscribe(socket, topic);
        return 'OK';
      case 'UNSUB':
        if (!topic) return '(error) ERR UNSUB needs a topic';
        unsubscribe(socket, topic);
        return 'OK';
      case 'PUB':
        if (!topic || !message) return '(error) ERR PUB needs a topic and a message';
        return `(integer) ${publish(topic, message)}`;
      case 'TOPICS':
        return topics.size ? [...topics.keys()].sort().join(' ') : '(empty list)';
      case '':
        return null; // blank line: ignore
      default:
        return `(error) ERR unknown command '${cmd}'`;
    }
  }

  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding('utf8');
    let buffer = ''; // TCP is a byte stream: buffer and split on "\n" (see miniredis)

    socket.on('data', (chunk) => {
      buffer += chunk;
      let i;
      while ((i = buffer.indexOf('\n')) !== -1) {
        const reply = handle(socket, buffer.slice(0, i));
        buffer = buffer.slice(i + 1);
        if (reply !== null) socket.write(reply + '\n');
      }
    });

    // CLEANUP matters here: a disconnected socket left in a topic's Set would
    // leak memory and make every later PUB try to write to a dead connection.
    const cleanup = () => {
      sockets.delete(socket);
      for (const topic of [...topics.keys()]) unsubscribe(socket, topic);
    };
    socket.on('error', () => {});
    socket.on('close', cleanup);
  });

  return {
    server,
    topics,
    listen: () => new Promise((resolve) => server.listen(port, host, () => resolve(server.address().port))),
    close() {
      for (const s of sockets) s.destroy();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

if (require.main === module) {
  const port = Number(process.env.PORT) || 7000;
  createBroker({ port })
    .listen()
    .then((p) => console.log(`minimq listening on 127.0.0.1:${p}`));
}

module.exports = { createBroker };
