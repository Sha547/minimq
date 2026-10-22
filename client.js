'use strict';

const net = require('node:net');
const readline = require('node:readline');

/*
 * client.js — interactive prompt. Unlike miniredis, output is not strictly
 * request/reply: "MSG ..." lines can arrive at any time. We print them on a
 * fresh line and redraw the prompt so they don't mangle what you're typing.
 */
const port = Number(process.argv[2]) || 7000;
const socket = net.connect({ port, host: '127.0.0.1' });
socket.setEncoding('utf8');

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'minimq> ' });
socket.on('connect', () => rl.prompt());

let buffer = '';
socket.on('data', (chunk) => {
  buffer += chunk;
  let i;
  while ((i = buffer.indexOf('\n')) !== -1) {
    readline.clearLine(process.stdout, 0);
    readline.cursorTo(process.stdout, 0);
    console.log(buffer.slice(0, i));
    buffer = buffer.slice(i + 1);
    rl.prompt(true); // true = keep what the user has typed so far
  }
});

socket.on('close', () => {
  console.log('\nDisconnected from broker.');
  process.exit(0);
});
socket.on('error', (err) => {
  console.error(err.code === 'ECONNREFUSED' ? `Could not connect on port ${port}. Is the broker running?` : err.message);
  process.exit(1);
});

rl.on('line', (line) => {
  if (/^\s*(exit|quit)\s*$/i.test(line)) return socket.end();
  if (line.trim()) socket.write(line + '\n');
  else rl.prompt();
});
rl.on('close', () => socket.end());
