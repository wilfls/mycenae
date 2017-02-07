CREATE TABLE IF NOT EXISTS mycenae.ts_keyspace (
    key text PRIMARY KEY,
    contact text,
    datacenter text,
    ks_ttl int,
    ks_tuuid boolean,
    name text,
    replication_factor int,
    replication_factor_meta text
);
CREATE INDEX IF NOT EXISTS ts_keyspace_name_index ON mycenae.ts_keyspace (name);

INSERT INTO mycenae.ts_keyspace (key, datacenter, name, replication_factor) VALUES ('mycenae', 'dc_gt_a1', 'mycenae', 3);


CREATE TABLE IF NOT EXISTS macs.ts_datacenter (
    datacenter text PRIMARY KEY
);

INSERT INTO mycenae.ts_datacenter (datacenter) VALUES ('dc_gt_a1');


CREATE TABLE IF NOT EXISTS mycenae.ts_error (
    tsid text,
    code int,
    date timestamp,
    error text,
    message text,
    PRIMARY KEY (tsid, code)
) WITH CLUSTERING ORDER BY (code ASC);


