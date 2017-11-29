CREATE KEYSPACE mycenae WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE KEYSPACE stats WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS mycenae.ts_keyspace (key text PRIMARY KEY, contact text, datacenter text, ks_ttl int, ks_tuuid boolean, name text, replication_factor int, replication_factor_meta text);

CREATE TABLE IF NOT EXISTS mycenae.ts_datacenter (datacenter text PRIMARY KEY);

CREATE TABLE IF NOT EXISTS mycenae.ts_error (tsid text, code int, date timestamp, error text, message text, PRIMARY KEY (tsid, code)) WITH CLUSTERING ORDER BY (code ASC);

CREATE TABLE stats.ts_text_stamp (
    id text,
    date timestamp,
    value text,
    PRIMARY KEY (id, date)
) WITH CLUSTERING ORDER BY (date ASC)
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '7', 'compaction_window_unit': 'DAYS', 'max_threshold': '64', 'min_threshold': '8'}
    AND dclocal_read_repair_chance = 1.0
    AND default_time_to_live = 43200
    AND gc_grace_seconds = 0
    AND read_repair_chance = 1.0
    AND speculative_retry = 'NONE';

CREATE TABLE IF NOT EXISTS stats.timeseries (
    id text,
    date timestamp,
    value blob,
    PRIMARY KEY (id, date)
)
WITH CLUSTERING ORDER BY (date ASC)
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '7', 'compaction_window_unit': 'DAYS', 'max_threshold': '64', 'min_threshold': '8'}
    AND dclocal_read_repair_chance = 1.0
    AND default_time_to_live = 43200
    AND gc_grace_seconds = 0
    AND read_repair_chance = 1.0
    AND speculative_retry = 'NONE';

INSERT INTO mycenae.ts_keyspace (key, datacenter, name, replication_factor) VALUES ('mycenae', 'datacenter1', 'mycenae', 3);

INSERT INTO mycenae.ts_datacenter (datacenter) VALUES ('datacenter1');

INSERT INTO mycenae.ts_keyspace (key, datacenter, ks_ttl, ks_tuuid, name, replication_factor) VALUES ('stats', 'datacenter1', 3, false, 'stats', 3);