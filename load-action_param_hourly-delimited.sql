-- Populate action_param_hourly from the wmf_raw.ApiAction table
--
-- Parameters:
--     action - action to restrict query to
--     param  - parameter to split and load
--     year  - year of partition to query
--     month - month of partition to query
--     day   - day of partition to query
--     hour  - hour of partition tp query
--
-- Usage:
-- hive --hiveconf hive.aux.jars.path= \
--      --database bd808 \
--      -f load-action_param_hourly-delimited.sql \
--      -d action=query \
--      -d param=prop \
--      -d year=2016 \
--      -d month=4 \
--      -d day=1 \
--      -d hour=0
--

ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION network_origin AS 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';

INSERT INTO TABLE action_param_hourly
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT
  params['action'] AS action,
  '${param}' AS param,
  prop AS value,
  wiki,
  network_origin(ip) ipClass,
  COUNT(*) viewCount
FROM
  wmf_raw.ApiAction
  LATERAL VIEW EXPLODE(SPLIT(params['${param}'], '\\|')) props as prop
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
  AND hadError = false
  AND params['action'] = '${action}'
GROUP BY
  params['action'],
  prop,
  wiki,
  network_origin(ip)
;
