#!/usr/bin/python

# Regression coverage for the Redis Lua compare-and-set highscore path.
# The bug this test guards: HighscoreReport keeps a local m_entries cache,
# but when Redis already holds a better score that the local cache doesn't
# know about (stale daemon, fresh replica, restart-in-progress), the old
# bare ZADD would overwrite it. The Lua script must reject the worse value
# even when the local cache thinks the write is the first one.

from common.unittests import StatsTest
from common.tlopostats import Daemon

import unittest


class TestAtomicHighscore(StatsTest):
    def test_lua_rejects_stale_lower_score(self):
        d = Daemon()
        d.start()

        self.doRPC('add_highscore', name='cd_wave', event='CD_WAVE', reversed=False)

        # Pre-seed Redis with a high score AFTER the daemon has loaded its
        # in-memory cache. The daemon's m_entries is now stale and will
        # think any incoming value is the first one for this avatar.
        key = Daemon.DATABASE + ':avatar:cd_wave'
        self.client.zadd(key, {'1234': 100})

        # Send a worse score. Daemon's cache says "av 1234 has nothing,
        # 50 beats nothing, write it." Without the Lua fix, the bare ZADD
        # would overwrite 100 with 50. With the fix, the EVAL script reads
        # ZSCORE, sees 100, and refuses the lower value.
        self.sendEvent('CD_WAVE', [1234], 50)

        self.expectHighscore('cd_wave', 1234, 100)

        d.stop()
        self.resetDatabase()

    def test_lua_accepts_higher_score_when_stale(self):
        d = Daemon()
        d.start()

        self.doRPC('add_highscore', name='cd_wave', event='CD_WAVE', reversed=False)

        # Same setup as above (stale cache) but the new value DOES beat
        # the existing one. Lua should accept the write.
        key = Daemon.DATABASE + ':avatar:cd_wave'
        self.client.zadd(key, {'1234': 100})

        self.sendEvent('CD_WAVE', [1234], 200)

        self.expectHighscore('cd_wave', 1234, 200)

        d.stop()
        self.resetDatabase()

    def test_lua_rejects_stale_higher_score_for_reversed(self):
        # Reversed leaderboard (lap-time style, lower is better).
        d = Daemon()
        d.start()

        self.doRPC('add_highscore', name='bp_time', event='BP_TIME', reversed=True)

        key = Daemon.DATABASE + ':avatar:bp_time'
        self.client.zadd(key, {'1234': 50})

        # 100 is worse than 50 in a reversed leaderboard. Lua must refuse.
        self.sendEvent('BP_TIME', [1234], 100)

        self.expectHighscore('bp_time', 1234, 50)

        d.stop()
        self.resetDatabase()

    def test_lua_writes_first_entry_when_no_prior_value(self):
        # Sanity: the script's `cur == false` branch (nil from ZSCORE) must
        # accept the write so brand-new avatars get on the board.
        d = Daemon()
        d.start()

        self.doRPC('add_highscore', name='cd_wave', event='CD_WAVE', reversed=False)

        self.sendEvent('CD_WAVE', [9999], 42)

        self.expectHighscore('cd_wave', 9999, 42)

        d.stop()
        self.resetDatabase()


if __name__ == '__main__':
    unittest.main()
