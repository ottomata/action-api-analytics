#!/usr/bin/env bash

set -eu -o pipefail

DB=bd808
DAY=$(date +%Y-%m-%d -d yesterday)
MONTH=$(date +%Y-%m -d yesterday)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

./load.py --db $DB -s $DAY load-action_ua_hourly.sql
./load.py --db $DB -s $DAY load-action_action_hourly.sql
./load.py --db $DB -s $DAY load-action_param_hourly.sql
./load.py --db $DB -s $DAY -d action=query -d param=prop load-action_param_hourly-delimited.sql
./load.py --db $DB -s $DAY -d action=query -d param=list load-action_param_hourly-delimited.sql
./load.py --db $DB -s $DAY -d action=query -d param=meta load-action_param_hourly-delimited.sql

./daily-api.py -s $DAY
tail -n +2 out/daily-${DAY}.tsv >> out/monthly-${MONTH}.tsv
