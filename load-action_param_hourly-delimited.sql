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
  `database` AS wiki,
  network_origin(http.client_ip) AS ipClass,
  COUNT(1) AS viewCount
FROM
  event.mediawiki_api_request
  LATERAL VIEW EXPLODE(SPLIT(params['${param}'], '\\|')) props as prop
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
  AND (api_error_codes IS NULL OR size(api_error_codes) = 0)
  AND params['action'] = '${action}'
GROUP BY
  params['action'],
  prop,
  `database`,
  network_origin(http.client_ip)
limit 100
;
