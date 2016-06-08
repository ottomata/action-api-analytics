-- Populate action_param_hourly from the wmf_raw.ApiAction table
--
-- Parameters:
--     year  - year of partition to query
--     month - month of partition to query
--     day   - day of partition to query
--     hour  - hour of partition tp query
--
-- Usage:
-- hive --hiveconf hive.aux.jars.path= \
--      --database bd808 \
--      -f load-action_action_hourly.sql \
--      -d year=2015 \
--      -d month=11 \
--      -d day=1 \
--      -d hour=0
--

ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION network_origin AS 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';

INSERT INTO TABLE action_param_hourly
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT
  COALESCE(params['action'], 'help') action,
  pTable.key AS param,
  pTable.value AS value,
  wiki,
  network_origin(ip) ipClass,
  COUNT(1) viewCount
FROM
  wmf_raw.ApiAction
  LATERAL VIEW EXPLODE(params) pTable AS key, value
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
  AND hadError = false
  AND (
    (params['action'] = 'flow' AND pTable.key = 'submodule')
    OR pTable.key = 'generator'
  )
GROUP BY
  COALESCE(params['action'], 'help'),
  pTable.key,
  pTable.value,
  wiki,
  network_origin(ip)
;
