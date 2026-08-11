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

-- Query to retrieve storage breakdown metrics from TABLE_STORAGE_METRICS
-- Used by GetStatistics in exact mode to get active/time-travel/failsafe byte counts
-- Note: Requires appropriate permissions (ACCOUNTADMIN role)
-- The IN clause (%s) is populated dynamically with (schema, table) pairs
SELECT table_schema, table_name,
       COALESCE(active_bytes, 0) as active_bytes,
       COALESCE(time_travel_bytes, 0) as time_travel_bytes,
       COALESCE(failsafe_bytes, 0) as failsafe_bytes
FROM %s.information_schema.table_storage_metrics
WHERE (table_schema, table_name) IN (%s)
ORDER BY table_schema, table_name
