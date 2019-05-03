-- Populate action_ua_hourly from the wmf_raw.ApiAction table
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
--      -f load-action_ua_hourly.sql \
--      -d year=2015 \
--      -d month=11 \
--      -d day=1 \
--      -d hour=0
--

ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION network_origin AS 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';

INSERT INTO TABLE action_ua_hourly
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT
    http.request_headers['user-agent'] AS userAgent,
    `database` AS wiki,
    network_origin(http.client_ip) AS ip,
    COUNT(1)
FROM event.mediawiki_api_request
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
GROUP BY
    http.request_headers['user-agent'],
    `database`,
    network_origin(http.client_ip)
;
