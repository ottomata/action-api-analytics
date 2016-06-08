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
    userAgent,
    wiki,
    network_origin(ip),
    COUNT(1)
FROM wmf_raw.ApiAction
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
GROUP BY
    userAgent,
    wiki,
    network_origin(ip)
;
