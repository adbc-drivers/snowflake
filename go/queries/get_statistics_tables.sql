-- Copyright (c) 2026 ADBC Drivers Contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Query to retrieve table information from INFORMATION_SCHEMA.TABLES
-- Used by GetStatistics to enumerate tables and their base statistics
-- Parameters: ? = schema filter, ? = table filter
SELECT table_catalog, table_schema, table_name,
       COALESCE(row_count, 0) as row_count,
       COALESCE(bytes, 0) as bytes,
       COALESCE(retention_time, 1) as retention_time,
       clustering_key
FROM %s.information_schema.tables
WHERE table_schema ILIKE ?
  AND table_name ILIKE ?
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name
