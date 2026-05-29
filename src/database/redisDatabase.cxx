#include "database.h"

#include "debug/DLog.h"
#include "collector/statCollector.h"
#include "collector/incrementalStatCollector.h"
#include "collector/highscoreCollector.h"

#include <jansson.h>

#include <redox.hpp>

#include <algorithm>
#include <atomic>
#include <ctime>
#include <exception>
#include <mutex>

using namespace redox;

// redox is a one-shot client: its event loop thread exits when the connection
// drops and the instance cannot be re-connect()ed (a later command throws
// "Need to connect Redox before running commands!"). So we hold it behind a
// pointer and, on reconnect, build a *fresh* instance and swap it in.
//
// Threading: g_rdx is only ever dereferenced on the main (io_service) thread
// (command issuance and reconnect both happen there). redox's own command and
// connection callbacks run on redox's event loop thread and touch only the
// atomics below, never g_rdx or the reconnect mutex, so there is no lock
// ordering hazard when we tear down the old instance under the mutex.
Redox* g_rdx = new Redox();
DLog dlog;

class RedisDatabase : public Database {
    public:
        RedisDatabase(const std::string& addr, int port,
                      const std::string& prefix) : m_addr(addr),
                        m_port(port), m_prefix(prefix)
        {
            connect_or_abort();
        }

        virtual ~RedisDatabase()
        {
        }

        virtual void get_collectors(boost::asio::io_service& io_service, collector_map_t& map)
        {
            dlog << "get_collectors";
            if (!ensure_connected())
            {
                dlog << "get_collectors skipped: redis unavailable";
                return;
            }

            try
            {
                auto& c = g_rdx->commandSync<std::unordered_set<std::string>>(
                    {"LRANGE", m_prefix + ":collectors", "0", "-1"}
                );

                if (c.ok())
                {
                    dlog << "get_collectors success";
                    for (auto data : c.reply())
                    {
                        json_error_t error;
                        json_t* value = json_loads(data.c_str(), 0, &error);
                        if (!value || !json_is_object(value))
                        {
                            std::cerr << "ignoring invalid data in collectors" << std::endl;
                            continue;
                        }

                        std::string name = json_string_value(json_object_get(value, "name"));
                        std::string event = json_string_value(json_object_get(value, "event"));
                        std::string type = json_string_value(json_object_get(value, "type"));
                        if (type == "periodic")
                        {
                            unsigned int period = json_integer_value(json_object_get(value, "period"));
                            map[name] = new StatCollector(name, event, this, period, io_service);
                        }

                        else if (type == "incremental")
                        {
                            map[name] = new IncrementalStatCollector(name, event, this, io_service);
                        }

                        else if (type == "highscore")
                        {
                            bool reversed = json_is_true(json_object_get(value, "reversed"));
                            map[name] = new HighscoreCollector(name, event, this, reversed, io_service);
                        }

                        else
                        {
                            std::cerr << "ignoring unknown collector type " << type << std::endl;
                            json_decref(value);
                            continue;
                        }

                        map[name]->start();
                        json_decref(value);
                    }
                } else {
                    dlog << "get_collectors failed";
                }

                c.free();
            }
            catch (const std::exception& e)
            {
                dlog << "get_collectors threw, redis dropped";
                m_connected = false;
            }
        }

        virtual void add_collector(StatCollectorBase* collector)
        {
            dlog << "add_collector";
            json_t* item = json_object();
            collector->write_json(item);

            char* data = json_dumps(item, 0);
            std::string payload(data);
            free(data);
            json_decref(item);

            m_pending++;
            issue({"RPUSH", m_prefix + ":collectors", payload});
        }

        virtual void remove_collector(StatCollectorBase* collector)
        {
            dlog << "remove_collector";
            json_t* item = json_object();
            collector->write_json(item);

            char* data = json_dumps(item, 0);
            std::string payload(data);
            free(data);
            json_decref(item);

            m_pending++;
            issue({"LREM", m_prefix + ":collectors", "0", payload});
        }

        virtual void get_ban_list(doid_list_t& list)
        {
            dlog << "get_ban_list";
            if (!ensure_connected())
            {
                dlog << "get_ban_list skipped: redis unavailable";
                return;
            }

            try
            {
                auto& c = g_rdx->commandSync<std::unordered_set<std::string>>(
                    {"LRANGE", m_prefix + ":banned", "0", "-1"}
                );

                if (c.ok()) {
                    dlog << "get_ban_list success";
                    for (auto id : c.reply())
                        list.insert(atoi(id.c_str()));
                } else {
                    dlog << "get_ban_list failed";
                }

                c.free();
            }
            catch (const std::exception& e)
            {
                dlog << "get_ban_list threw, redis dropped";
                m_connected = false;
            }
        }

        virtual void add_to_ban_list(doid_t id)
        {
            dlog << "add_to_ban_list";
            if (!ensure_connected())
            {
                dlog << "add_to_ban_list skipped: redis unavailable";
                return;
            }

            m_pending++;
            issue({"RPUSH", m_prefix + ":banned", std::to_string(id)});

            // Remove id from all leaderboards
            auto callback = [id](Command<std::unordered_set<std::string>>& c)
            {
                if (!c.ok())
                    return;

                dlog << "add_to_ban_list callback";
                for (auto key : c.reply())
                    g_rdx->command<int>({"ZREM", key, std::to_string(id)});
            };

            try
            {
                g_rdx->command<std::unordered_set<std::string>>({"KEYS", m_prefix + ":avatar:*"},
                                                                callback);
                g_rdx->command<std::unordered_set<std::string>>({"KEYS", m_prefix + ":guild:*"},
                                                                callback);
            }
            catch (const std::exception& e)
            {
                dlog << "add_to_ban_list threw, redis dropped";
                m_connected = false;
            }
        }

        virtual void get_guild_map(guild_map_t& map)
        {
            dlog << "get_guild_map";
            if (!ensure_connected())
            {
                dlog << "get_guild_map skipped: redis unavailable";
                return;
            }

            try
            {
                auto& c = g_rdx->commandSync<std::vector<std::string>>(
                    {"HGETALL", m_prefix + ":guilds"}
                );

                if (c.ok())
                {
                    dlog << "get_guild_map success";
                    auto vec = c.reply();
                    auto it = vec.begin();
                    while (it != vec.end())
                    {
                        doid_t k = atoi((*it++).c_str());
                        doid_t v = atoi((*it++).c_str());
                        map[k] = v;
                    }
                } else {
                    dlog << "get_guild_map fail";
                }

                c.free();
            }
            catch (const std::exception& e)
            {
                dlog << "get_guild_map threw, redis dropped";
                m_connected = false;
            }
        }

        virtual void add_to_guild_map(doid_t av, doid_t guild)
        {
            dlog << "add_to_guild_map";
            m_pending++;
            issue({"HSET", m_prefix + ":guilds",
                   std::to_string(av),
                   std::to_string(guild)});
        }

        virtual void add_entry(const std::string& name,
                               const std::string& type,
                               doid_t key,
                               long value)
        {
            dlog << "add_entry";
            if (drop_if_queue_full("add_entry"))
                return;

            json_t* item = json_object();
            json_object_set_new(item, "name", json_string(name.c_str()));
            json_object_set_new(item, "type", json_string(type.c_str()));
            json_object_set_new(item, "key", json_integer(key));
            json_object_set_new(item, "value", json_integer(value));

            char timestamp[64];
            time_t rawtime;
            time(&rawtime);
            strftime(timestamp, 64, "%d/%b/%Y:%H:%M:%S %z", localtime(&rawtime));
            json_object_set_new(item, "time", json_string(timestamp));

            char* data = json_dumps(item, 0);
            std::string payload(data);
            free(data);
            json_decref(item);

            m_pending++;
            issue({"RPUSH", m_prefix + ":events", payload});
        }

        virtual void add_incremental_report(const std::string& collection,
                                            doid_t key,
                                            long value)
        {
            if (drop_if_queue_full("add_incremental_report"))
                return;

            m_pending++;
            issue({"ZINCRBY",
                   m_prefix + ":" + collection,
                   std::to_string(value),
                   std::to_string(key)});
        }

        virtual void load_highscore_entries(const std::string& collection,
                                            std::unordered_map<doid_t, long>& entries)
        {
            dlog << "load_highscore_entries";
            if (!ensure_connected())
            {
                dlog << "load_highscore_entries skipped: redis unavailable";
                return;
            }

            try
            {
                auto& c = g_rdx->commandSync<std::vector<std::string>>(
                    {"ZRANGE", m_prefix + ":avatar:" + collection, "0", "-1", "WITHSCORES"}
                );

                if (c.ok())
                {
                    dlog << "load_highscore_entries success";
                    auto vec = c.reply();
                    auto it = vec.begin();
                    while (it != vec.end())
                    {
                        doid_t k = atoi((*it++).c_str());
                        long v = atol((*it++).c_str());
                        entries[k] = v;
                    }
                } else {
                    dlog << "load_highscore_entries failed";
                }

                c.free();
            }
            catch (const std::exception& e)
            {
                dlog << "load_highscore_entries threw, redis dropped";
                m_connected = false;
            }
        }

        virtual void set_highscore_entry(const std::string& collection,
                                         doid_t key,
                                         long value)
        {
            dlog << "set_highscore_entry";
            if (drop_if_queue_full("set_highscore_entry"))
                return;

            m_pending++;
            issue({"ZADD",
                   m_prefix + ":avatar:" + collection,
                   std::to_string(value),
                   std::to_string(key)});
        }

    private:
        // Bounded pending-command queue. While Redis is reachable but slow,
        // in-flight commands pile up; cap how many we let accumulate so a
        // backlog can't grow without bound.
        static constexpr int MAX_PENDING = 10000;

        // Exponential backoff cap (seconds) between reconnection attempts.
        static constexpr int MAX_BACKOFF_SECONDS = 60;

        bool drop_if_queue_full(const char* op)
        {
            if (m_pending < MAX_PENDING)
                return false;

            std::cerr << "redis: pending queue full (" << m_pending
                      << "), dropping " << op << std::endl;
            return true;
        }

        // Issue a fire-and-forget write. Tolerates a dropped connection without
        // throwing: if we're disconnected we account for the drop and return,
        // and if the command throws because the socket died between the check
        // and the call we swallow it and mark the connection down. Either way
        // the daemon stays up and starts flowing again on the next reconnect.
        void issue(const std::vector<std::string>& cmd)
        {
            if (!ensure_connected())
            {
                m_pending--;
                return;
            }

            try
            {
                g_rdx->command<redisReply*>(cmd, [this](Command<redisReply*>& c) {
                    if (!c.ok())
                    {
                        dlog << "command failed; marking redis down";
                        m_connected = false;
                    }
                    m_pending--;
                });
            }
            catch (const std::exception& e)
            {
                dlog << "command threw; marking redis down";
                m_connected = false;
                m_pending--;
            }
        }

        // Ensure g_rdx points at a connected instance, recreating it if needed.
        // redox cannot be re-connect()ed after a drop, so we build a fresh one
        // and swap it in, then tear down the dead instance. Reconnect attempts
        // are rate-limited by an exponential backoff so a long outage doesn't
        // turn into a tight connect loop. Runs on the main thread.
        bool ensure_connected()
        {
            if (m_connected)
                return true;

            std::lock_guard<std::mutex> lock(m_reconnect_mu);
            if (m_connected)
                return true;

            time_t now = time(nullptr);
            if (now - m_last_attempt < m_backoff_seconds)
                return false;
            m_last_attempt = now;

            std::cerr << "redis: attempting reconnect (backoff "
                      << m_backoff_seconds << "s, " << m_pending
                      << " pending)" << std::endl;

            Redox* fresh = new Redox();
            if (fresh->connect(m_addr, m_port,
                               [this](int state) { m_connected = (state == Redox::CONNECTED); }))
            {
                Redox* dead = g_rdx;
                g_rdx = fresh;
                if (dead)
                {
                    dead->disconnect();
                    delete dead;
                }
                m_connected = true;
                m_backoff_seconds = 1;
                std::cerr << "redis: reconnected" << std::endl;
                return true;
            }

            delete fresh;
            m_backoff_seconds = std::min(m_backoff_seconds * 2, MAX_BACKOFF_SECONDS);
            return false;
        }

        void connect_or_abort()
        {
            dlog << "connect_or_abort";
            if (!g_rdx->connect(m_addr, m_port,
                                [this](int state) { m_connected = (state == Redox::CONNECTED); }))
            {
                dlog << "unable to connect to Redis server";
                std::cerr << "unable to connect to Redis server" << std::endl;
                exit(1);
            }
            m_connected = true;
        }

        std::string m_addr;
        int m_port;
        std::string m_prefix;
        std::atomic<int> m_pending{0};
        std::atomic<bool> m_connected{false};
        std::mutex m_reconnect_mu;
        time_t m_last_attempt = 0;
        int m_backoff_seconds = 1;
};

// Out-of-class definitions required for ODR-used static constexpr members
// when compiling under C++14 (Alpine 3.8's gcc 6 default). Harmless and
// redundant under C++17, which makes static constexpr members implicitly
// inline. Without these the linker fails with undefined references when
// ensure_connected takes the address of MAX_BACKOFF_SECONDS via std::min.
constexpr int RedisDatabase::MAX_PENDING;
constexpr int RedisDatabase::MAX_BACKOFF_SECONDS;

Database* get_redis_db(const std::string& addr, int port,
                       const std::string& prefix)
{
    return new RedisDatabase(addr, port, prefix);
}
