CREATE TABLE test_numeric_high_precision (
    idx INTEGER,
    res NUMERIC(38,0)
);

INSERT INTO test_numeric_high_precision (idx, res) VALUES (1, 1);
INSERT INTO test_numeric_high_precision (idx, res) VALUES (2, 9223372036854775807);
INSERT INTO test_numeric_high_precision (idx, res) VALUES (3, -9223372036854775808);
INSERT INTO test_numeric_high_precision (idx, res) VALUES (4, 12345678901234567890123456789012345678);
INSERT INTO test_numeric_high_precision (idx, res) VALUES (5, -12345678901234567890123456789012345678);
INSERT INTO test_numeric_high_precision (idx, res) VALUES (6, NULL);
