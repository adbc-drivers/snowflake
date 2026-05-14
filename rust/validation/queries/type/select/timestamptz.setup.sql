CREATE TABLE test_timestamptz (
    idx INTEGER,
    res TIMESTAMP_TZ(6)
);

INSERT INTO test_timestamptz (idx, res) VALUES (1, '2023-05-15 13:45:30+00:00'::TIMESTAMP_TZ);
INSERT INTO test_timestamptz (idx, res) VALUES (2, '2000-01-01 00:00:00+00:00'::TIMESTAMP_TZ);
INSERT INTO test_timestamptz (idx, res) VALUES (3, '1969-07-20 20:17:40+00:00'::TIMESTAMP_TZ);
INSERT INTO test_timestamptz (idx, res) VALUES (4, '9999-12-31 23:59:59+00:00'::TIMESTAMP_TZ);
INSERT INTO test_timestamptz (idx, res) VALUES (5, NULL);
