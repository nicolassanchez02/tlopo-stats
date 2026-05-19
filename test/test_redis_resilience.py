#!/usr/bin/python

# Regression coverage for the Redis-outage durability changes.
# The bug this test guards: the old daemon called exit(1) after three
# failed reconnect attempts, silently dropping every event whose
# callback was waiting on RPUSH / ZINCRBY / ZADD. The new behaviour
# retries forever on exponential backoff, caps the pending-command
# queue at MAX_PENDING, and resumes processing once Redis returns.
#
# This test runs its own redis-server subprocess on a non-default port
# so it can stop/start Redis at will without disturbing the CI service
# container or the default 6379 instance used by other tests.

import json
import os
import socket
import subprocess
import time
import unittest

import redis


DAEMON_PATH = './tlopostats'
DATABASE = 'tlopo_stats_resilience'
REDIS_PORT = 6380
RPC_PORT = 8964


class TestRedisResilience(unittest.TestCase):
    def setUp(self):
        self.redis_proc = None
        self.daemon_proc = None
        self._start_redis()
        self.client = redis.Redis(port=REDIS_PORT)
        self._reset_db()
        self._start_daemon()
        # Configure a collector so events have somewhere to land.
        self._rpc('add_incremental', name='kills', event='ENEMY_KILLED')

    def tearDown(self):
        if self.daemon_proc:
            self.daemon_proc.kill()
            self.daemon_proc.wait()
        if self.redis_proc:
            self.redis_proc.terminate()
            # subprocess.TimeoutExpired doesn't exist in Py2.7 (which the
            # Alpine 3.8 CI image still uses). Poll for exit instead so
            # this works on both runtimes.
            for _ in range(50):
                if self.redis_proc.poll() is not None:
                    break
                time.sleep(0.1)
            else:
                self.redis_proc.kill()

    def _start_redis(self):
        # --save '' disables RDB snapshotting so the subprocess starts
        # clean every run and a stop+start can't be confused by an
        # auto-loaded dump.
        self.redis_proc = subprocess.Popen(
            ['redis-server', '--port', str(REDIS_PORT), '--save', '',
             '--appendonly', 'no', '--daemonize', 'no'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Wait until it accepts connections.
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                redis.Redis(port=REDIS_PORT).ping()
                return
            except redis.ConnectionError:
                time.sleep(0.1)
        raise RuntimeError('redis-server failed to start on port %d' % REDIS_PORT)

    def _stop_redis(self):
        if self.redis_proc and self.redis_proc.poll() is None:
            self.redis_proc.terminate()
            # Same Py2 compat note as in tearDown.
            for _ in range(50):
                if self.redis_proc.poll() is not None:
                    break
                time.sleep(0.1)
            else:
                self.redis_proc.kill()
        self.redis_proc = None

    def _start_daemon(self):
        self.daemon_proc = subprocess.Popen(
            [DAEMON_PATH, '--redis-db', '127.0.0.1', str(REDIS_PORT), DATABASE],
            stderr=subprocess.PIPE)
        time.sleep(1.0)

    def _reset_db(self):
        for key in self.client.keys(DATABASE + ':*'):
            self.client.delete(key)

    def _rpc(self, method, **kwargs):
        kwargs['method'] = method
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', RPC_PORT))
        sock.send((json.dumps(kwargs) + '\n').encode())
        return json.loads(sock.recv(1024))

    def _send_event(self, name, doIds, value):
        data = json.dumps({'event': name, 'doIds': doIds, 'value': value})
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data.encode(), ('127.0.0.1', 8963))

    def _events_in_queue(self):
        return self.client.llen(DATABASE + ':events')

    def test_daemon_survives_redis_outage_and_drains(self):
        # Pre-outage: a few events land normally.
        for _ in range(5):
            self._send_event('ENEMY_KILLED', [1234], 1)
        time.sleep(2)
        pre_outage = self._events_in_queue()
        self.assertGreater(pre_outage, 0)

        # Kill Redis. The daemon's in-flight callbacks now start
        # retrying on exponential backoff (1s -> 60s).
        self._stop_redis()
        time.sleep(2)

        # Daemon must still be alive (the regression: it used to exit(1)
        # after the third failed reconnect).
        self.assertIsNone(self.daemon_proc.poll(),
                          'daemon exited during Redis outage')

        # Bring Redis back on the same port.
        self._start_redis()
        self.client = redis.Redis(port=REDIS_PORT)

        # Send fresh events. They should flow through the freshly
        # reconnected daemon. Allow up to one full backoff window for
        # the daemon to notice Redis is back.
        deadline = time.time() + 65
        new_event_landed = False
        while time.time() < deadline:
            self._send_event('ENEMY_KILLED', [9999], 1)
            time.sleep(2)
            if self._events_in_queue() > 0:
                new_event_landed = True
                break
        self.assertTrue(new_event_landed,
                        'daemon never resumed sending events to Redis')

    def test_daemon_stays_alive_through_long_outage(self):
        # The old behaviour bailed out at ~3 retries. Confirm the daemon
        # is still up after a long enough wait to exhaust the legacy
        # retry budget several times over.
        self._stop_redis()
        time.sleep(20)
        self.assertIsNone(self.daemon_proc.poll(),
                          'daemon exited during long Redis outage')


if __name__ == '__main__':
    unittest.main()
