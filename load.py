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


def is_file(arg):
    try:
        open(arg, 'r')
    except IOError:
        raise argparse.ArgumentTypeError('File %s does not exist' % x)
    return arg


parser = argparse.ArgumentParser(
    description='Run an HSQL command once for each hour in the specified date range.')
parser.add_argument('--debug', dest='debug', action='store_true',
    help='Show hive commands that would be run, but do not execute anything.')
parser.add_argument('--db', default='bd808',
    help='Target database')
parser.add_argument('-s', '--start',
    help='Start date (inclusive)', metavar='YYYY-MM-DD', required=True, type=parse_date)
parser.add_argument('-e', '--end',
    help='End date (exclusive)', metavar='YYYY-MM-DD', type=parse_date)
parser.add_argument('-d', '--define', dest='defines',
        action='append',
        help='Variable subsitution to apply to hive commands.',
        metavar='<key=value>')
parser.add_argument('hsql', metavar='HSQL', type=is_file,
    help='HSQL file to run to load database', )
args = parser.parse_args()

if not args.end:
    args.end = args.start + datetime.timedelta(1)

for d in daterange(args.start, args.end):
    for h in range(24):
        cmd = ('/usr/bin/hive '
            '--hiveconf hive.aux.jars.path= '
            '--database %(db)s '
            '-f %(hsql)s '
            '-d year=%(year)s '
            '-d month=%(month)s '
            '-d day=%(day)s '
            '-d hour=%(hour)s'
        ) % {
            'db': args.db,
            'hsql': args.hsql,
            'year': d.year,
            'month': d.month,
            'day': d.day,
            'hour': h
        }

        if args.defines:
            cmd = '%s -d %s' % (cmd, ' -d '.join(args.defines))

        if args.debug:
            print cmd
        else:
            subprocess.call(cmd, shell=True)
