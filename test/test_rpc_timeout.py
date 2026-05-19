#!/usr/bin/python

# Regression coverage for the RPC read deadline (RPC_READ_TIMEOUT_SECONDS).
# The bug this test guards: a client could connect, send nothing, and
# hold the connection slot open forever. A misbehaving peer opening
# sockets faster than the OS reclaims them could starve the game
# server's stat reporting.

from common.unittests import StatsTest
from common.tlopostats import Daemon

import os
import re
import socket
import time
import unittest


def _read_rpc_timeout_seconds():
    # Pull the canonical timeout out of net/globals.h so this test
    # stays accurate if the constant is ever tuned.
    here = os.path.dirname(os.path.abspath(__file__))
    header = os.path.join(here, '..', 'src', 'net', 'globals.h')
    pattern = re.compile(r'^\s*#define\s+RPC_READ_TIMEOUT_SECONDS\s+(\d+)')
    with open(header) as f:
        for line in f:
            m = pattern.match(line)
            if m:
                return int(m.group(1))
    raise RuntimeError('RPC_READ_TIMEOUT_SECONDS not found in globals.h')


TIMEOUT = _read_rpc_timeout_seconds()


class TestRPCTimeout(StatsTest):
    def test_silent_connection_drops_after_timeout(self):
        d = Daemon()
        d.start()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 8964))
        sock.settimeout(TIMEOUT + 10)

        start = time.time()
        # Daemon should close the socket from its end at the deadline.
        # recv returns b'' on a clean close; any other return means the
        # server actually sent data (it shouldn't have) or the deadline
        # never fired.
        data = sock.recv(1)
        elapsed = time.time() - start

        self.assertEqual(data, b'')
        # Lower bound: don't accept an instant close (would mask the bug).
        self.assertGreaterEqual(elapsed, TIMEOUT - 2)
        # Upper bound: don't let a misbehaving daemon hide behind the
        # outer settimeout safety net.
        self.assertLessEqual(elapsed, TIMEOUT + 5)

        sock.close()
        d.stop()
        self.resetDatabase()

    def test_valid_command_responds_normally(self):
        # The timeout must not break the happy path. A well-behaved
        # client that sends a request promptly should still get a
        # response within ordinary latency.
        d = Daemon()
        d.start()

        result = self.doRPC('list')
        self.assertTrue(result['success'])

        d.stop()
        self.resetDatabase()

    def test_late_data_after_timeout_is_dropped(self):
        # A client that connects, idles past the deadline, and only then
        # tries to send a command must not be served. The daemon has
        # already closed its side; the write may succeed locally (kernel
        # buffer) but no response can come back.
        d = Daemon()
        d.start()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 8964))
        sock.settimeout(TIMEOUT + 10)

        # Wait past the daemon-side deadline.
        time.sleep(TIMEOUT + 1)

        try:
            sock.send(b'{"method":"list"}\n')
        except socket.error:
            # Acceptable: daemon already closed, write got RST.
            pass

        # Whatever recv returns, it must not be a valid JSON response.
        sock.settimeout(2)
        try:
            data = sock.recv(1024)
        except socket.timeout:
            data = b''
        self.assertEqual(data, b'')

        sock.close()
        d.stop()
        self.resetDatabase()


if __name__ == '__main__':
    unittest.main()
