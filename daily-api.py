#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import subprocess

def daterange(sd, ed):
    for n in range(int((ed -sd).days)):
        yield sd + datetime.timedelta(n)


def parse_date(s):
    try:
        return datetime.datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError('Not a valid date: "%s"' % s)


parser = argparse.ArgumentParser(description='Daily visit count')
parser.add_argument('-s', '--start',
    help='Start date (inclusive)', metavar='YYYY-MM-DD', required=True, type=parse_date)
parser.add_argument('-e', '--end',
    help='End date (exclusive)', metavar='YYYY-MM-DD', type=parse_date)
args = parser.parse_args()

if not args.end:
    args.end = args.start + datetime.timedelta(1)

for d in daterange(args.start, args.end):
    cmd = ('/usr/bin/hive '
        '--database bd808 '
        '-f query-daily-api.sql '
        '-d year=%(year)s '
        '-d month=%(month)s '
        '-d day=%(day)s '
        '| tee out/daily-%(year)04d-%(month)02d-%(day)02d.tsv '
    ) % { 'year': d.year, 'month': d.month, 'day': d.day }
    #print cmd
    subprocess.call(cmd, shell=True)
