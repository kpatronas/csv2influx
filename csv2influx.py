#!/usr/bin/env python3
import os
import sys
import pandas as pd
from datetime import datetime
from argparse import ArgumentParser

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')

if __name__ == '__main__':

    parser = ArgumentParser(description = "CSV to inlfuxdb.")

    parser.add_argument("-c", dest = "CSV",                              \
                        required   = True,                               \
                        help       = "CSV input file", metavar="FILE",   \
                        type       = lambda x: is_valid_file(parser, x))

    parser.add_argument("-i", dest = "influxdb",                              \
                        required   = False,                                    \
                        help       = "influxdb options file", metavar="FILE", \
                        type       = lambda x: is_valid_file(parser, x))

    parser.add_argument("-o", dest = "options",                                  \
                        required   = False,                                      \
                        help       = "CSV processing options", metavar="FILE",   \
                        type       = lambda x: is_valid_file(parser, x))

    args = parser.parse_args()

    print(args)
    influxdb_options = args.influxdb.name
    csv_options      = args.options
    csv_file         = args.CSV

    df = pd.read_csv(csv_file)

    with open(influxdb_options) as f:
        db_options = f.readlines()

    from influxdb_client import InfluxDBClient, Point, WriteOptions
    from influxdb_client.client.write_api import SYNCHRONOUS

    influx_db_options = {}
    influx_db_options['host']         = "127.0.0.1"
    influx_db_options['port']         = '8086'
    influx_db_options['measurement']  = ""
    influx_db_options['database']     = ""
    influx_db_options['ts']           = ""
    influx_db_options['drop']         = ""
    influx_db_options['tags']         = ""
    influx_db_options['token']        = ""
    influx_db_options['org']          = ""

    for line in db_options:
        line  = line.replace('\n','')
        k,v   = line.split(':')
        influx_db_options[k] = v

    for i in ['measurement','database','tags']:
        if influx_db_options[i] == "":
            print('%s cannot be null'%(i))
            sys.exit(1)


    if len(influx_db_options['drop']) > 0:
        to_drop = influx_db_options['drop'].split(',')

    if len(influx_db_options['tags']) > 0:
        tags    = influx_db_options['tags'].split(',')

    if len(influx_db_options['drop']) > 0:
        df.drop(columns = to_drop, inplace = True)

    if influx_db_options['ts'] != "":
        df.set_index(influx_db_options['ts'],inplace = True)
    else:
        df['ts'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        df.set_index(['ts'],inplace = True)

    with InfluxDBClient(url="http://%s:%s"%(influx_db_options['host'],          \
                                            influx_db_options['port']),         \
                                            token = influx_db_options['token'], \
                                            org   = influx_db_options['org']) as _client:

        with _client.write_api(write_options=WriteOptions(batch_size=500)) as _write_client:
                    _write_client.write(influx_db_options['database'],                                 \
                                        "",                                                            \
                                        record = df,                                                   \
                                        data_frame_measurement_name = influx_db_options['measurement'],\
                                        data_frame_tag_columns      = tags)
