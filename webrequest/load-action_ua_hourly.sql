-- Populate action_ua_hourly from the wmf.webrequest table
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
CREATE TEMPORARY FUNCTION network_origin as 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';

INSERT INTO TABLE action_ua_hourly
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT
    user_agent,
    CONCAT(normalized_host.project, normalized_host.project_class),
    network_origin(client_ip),
    COUNT(*)
FROM wmf.webrequest
WHERE year = ${year}
  AND month = ${month}
  AND day = ${day}
  AND hour = ${hour}
  AND uri_path = '/w/api.php'
GROUP BY
    user_agent,
    CONCAT(normalized_host.project, normalized_host.project_class),
    network_origin(client_ip)
;
