CREATE TABLE test_numeric_low_precision (
    idx INTEGER,
    res NUMERIC(38,0)
);

INSERT INTO test_numeric_low_precision (idx, res) VALUES (1, 0);
INSERT INTO test_numeric_low_precision (idx, res) VALUES (2, 1);
INSERT INTO test_numeric_low_precision (idx, res) VALUES (3, -1);
INSERT INTO test_numeric_low_precision (idx, res) VALUES (4, 9223372036854775807);
INSERT INTO test_numeric_low_precision (idx, res) VALUES (5, -9223372036854775808);
INSERT INTO test_numeric_low_precision (idx, res) VALUES (6, NULL);
