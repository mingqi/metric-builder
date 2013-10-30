#!/usr/bin/python
import argparse, sys, re, json, os
from datetime import datetime, timedelta
import time, pytz
from os.path import join, dirname, abspath, exists


def setup_py_env():
    root_dir = dirname(__file__)
    sys.path.insert(0, join(root_dir, python))

def period_of(interval_type):
    if interval_type == '5m':
        period = 60 * 5
    elif interval_type == '1h':
        period = 60 * 60
    return period

def standardize_timepoint(time_point, interval_type):
    epoch_seconds = time.mktime(time_point.timetuple())
    period = period_of(interval_type)
    epoch_seconds = epoch_seconds - (epoch_seconds % period)
    return pytz.timezone("Asia/Shanghai").localize(datetime.fromtimestamp(epoch_seconds))

def timepoint_between(start, end, interval_type):
    result = []
    period = period_of(interval_type)
    s = standardize_timepoint(start, interval_type)
    e = standardize_timepoint(end, interval_type)
    while( s <= e ):
        result.append(s)    
        s = s + timedelta(seconds=period)

    return result



def parse_time(time_str):
    if re.match(r'\d{4}-\d{2}-\d{2}/\d{2}:\d{2}', time_str):
        return datetime.strptime(time_str,'%Y-%m-%d/%H:%M')
    elif re.match(r'\d{4}-\d{2}-\d{2}/\d{2}', time_str):
        return datetime.strptime(time_str,'%Y-%m-%d/%H')
    else:
        raise ValueError('time format is illegal')

def parse_args(args):
    arg_parser = argparse.ArgumentParser(description = 'process TSD files by hadoop')    
    arg_parser.add_argument('-s', '--start', required=True, type=parse_time, help='start time point YYYY-MM-DD/HH:mm]')
    arg_parser.add_argument('-e', '--end', required=True, type=parse_time, help='end time point YYYY-MM-DD/HH:mm')
    arg_parser.add_argument('-i', '--interval', required=True, help='interval type [5m|1h]')
    return arg_parser.parse_args(args)

def main():
    args = parse_args(sys.argv[1:])
    for timepoint in timepoint_between(args.start, args.end, args.interval):
   		job = hadoop.	

if __name__ == '__main__':
	main()
