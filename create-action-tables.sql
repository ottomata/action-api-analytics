-- Create tables for Action API stats
--
-- Usage:
--     hive -f create-action-tables.sql --database wmf

CREATE TABLE IF NOT EXISTS action_ua_hourly (
  userAgent STRING COMMENT 'Raw user-agent',
  wiki      STRING COMMENT 'Target wiki (e.g. enwiki)',
  ipClass   STRING COMMENT 'IP based origin, can be wikimedia, wikimedia_labs or internet',
  viewCount BIGINT COMMENT 'Number of requests'
)
COMMENT 'Hourly summary of Action API requests bucketed by user-agent and wiki'
PARTITIONED BY (
  year      INT COMMENT 'Unpadded year of request',
  month     INT COMMENT 'Unpadded month of request',
  day       INT COMMENT 'Unpadded day of request',
  hour      INT COMMENT 'Unpadded hour of request'
)
STORED AS PARQUET;


CREATE EXTERNAL TABLE IF NOT EXISTS action_action_hourly (
  action    STRING COMMENT 'Action parameter value',
  wiki      STRING COMMENT 'Target wiki (e.g. enwiki)',
  ipClass   STRING COMMENT 'IP based origin, can be wikimedia, wikimedia_labs or internet',
  viewCount BIGINT COMMENT 'Number of requests'
)
COMMENT 'Hourly summary of Action API requests bucketed by action and wiki'
PARTITIONED BY (
  year      INT COMMENT 'Unpadded year of request',
  month     INT COMMENT 'Unpadded month of request',
  day       INT COMMENT 'Unpadded day of request',
  hour      INT COMMENT 'Unpadded hour of request'
)
STORED AS PARQUET;


CREATE EXTERNAL TABLE IF NOT EXISTS action_param_hourly (
  action    STRING COMMENT 'Action parameter value',
  param     STRING COMMENT 'Parameter name, can be prop, list, meta, generator, etc',
  value     STRING COMMENT 'Parameter value',
  wiki      STRING COMMENT 'Target wiki (e.g. enwiki)',
  ipClass   STRING COMMENT 'IP based origin, can be wikimedia, wikimedia_labs or internet',
  viewCount BIGINT COMMENT 'Number of requests'
)
COMMENT 'Hourly summary of Action API requests bucketed by action, parameter, value and wiki'
PARTITIONED BY (
  year      INT COMMENT 'Unpadded year of request',
  month     INT COMMENT 'Unpadded month of request',
  day       INT COMMENT 'Unpadded day of request',
  hour      INT COMMENT 'Unpadded hour of request'
)
STORED AS PARQUET;

-- NOTE: there are many params we do not want to count distinct values of
-- at all (eg maxlag, smaxage, maxage, requestid, origin, centralauthtoken,
-- titles, pageids). Rather than trying to make an extensive blacklist and
-- potentially allow new parameters to slip through which have high
-- cardinality or sensitive information, the ETL process will use a whitelist
-- approach to count params that have been deemed to be useful.
--
-- The initial whitelist is (query, prop), (query, list), (query, meta),
-- (flow, module), (*, generator). The prop, list and meta parameters will
-- additionally be split on '|' with each component counted separately.
