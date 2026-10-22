'use strict';

// Run with: node test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { createBroker } = require('./broker');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Test client that collects every line it receives, in order. */
function connect(port) {
  return new Promise((resolve) => {
    const socket = net.connect(port, '127.0.0.1');
    socket.setEncoding('utf8');
    const lines = [];
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk;
      let i;
      while ((i = buffer.indexOf('\n')) !== -1) {
        lines.push(buffer.slice(0, i));
        buffer = buffer.slice(i + 1);
      }
    });
    const send = async (line) => {
      socket.write(line + '\n');
      await sleep(40); // let the broker respond; fine for a localhost test
    };
    socket.on('connect', () => resolve({ send, lines, raw: (s) => socket.write(s), close: () => socket.destroy() }));
  });
}

async function start() {
  const broker = createBroker({ port: 0 });
  return { broker, port: await broker.listen() };
}

test('subscribers receive published messages, others do not', async () => {
  const { broker, port } = await start();
  const [sub1, sub2, other, pub] = await Promise.all([connect(port), connect(port), connect(port), connect(port)]);
  await sub1.send('SUB news');
  await sub2.send('SUB news');
  await other.send('SUB sports');
  await pub.send('PUB news hello big world');
  assert.deepEqual(sub1.lines, ['OK', 'MSG news hello big world']);
  assert.deepEqual(sub2.lines, ['OK', 'MSG news hello big world']);
  assert.deepEqual(other.lines, ['OK']);
  assert.deepEqual(pub.lines, ['(integer) 2']);
  [sub1, sub2, other, pub].forEach((c) => c.close());
  await broker.close();
});

test('publishing with no subscribers reaches nobody and is not stored', async () => {
  const { broker, port } = await start();
  const pub = await connect(port);
  await pub.send('PUB void anyone there');
  assert.deepEqual(pub.lines, ['(integer) 0']);
  const late = await connect(port);
  await late.send('SUB void');
  assert.deepEqual(late.lines, ['OK']); // no replay of the earlier message
  pub.close();
  late.close();
  await broker.close();
});

test('UNSUB stops delivery; TOPICS lists active topics', async () => {
  const { broker, port } = await start();
  const sub = await connect(port);
  const pub = await connect(port);
  await sub.send('SUB b');
  await sub.send('SUB a');
  await sub.send('TOPICS');
  await sub.send('UNSUB a');
  await pub.send('PUB a nope');
  await pub.send('TOPICS');
  assert.deepEqual(sub.lines, ['OK', 'OK', 'a b', 'OK']);
  assert.deepEqual(pub.lines, ['(integer) 0', 'b']);
  sub.close();
  pub.close();
  await broker.close();
});

test('disconnected subscribers are cleaned up', async () => {
  const { broker, port } = await start();
  const sub = await connect(port);
  await sub.send('SUB temp');
  assert.equal(broker.topics.size, 1);
  sub.close();
  await sleep(60);
  assert.equal(broker.topics.size, 0);
  await broker.close();
});

test('framing, errors and unknown commands', async () => {
  const { broker, port } = await start();
  const c = await connect(port);
  c.raw('PI');
  await sleep(20);
  c.raw('NG\nSU');
  await sleep(20);
  c.raw('B t\nPUB t\nBOGUS\n');
  await sleep(60);
  assert.deepEqual(c.lines, ['PONG', 'OK', '(error) ERR PUB needs a topic and a message', "(error) ERR unknown command 'BOGUS'"]);
  c.close();
  await broker.close();
});
