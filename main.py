import argparse
import csv
import logging
import getpass
from collections import OrderedDict
from pprint import pprint

import coloredlogs
from dbfread import read, DBF
import MySQLdb
import progressbar
 
import logger

global TYPE_MAP

TYPE_MAP = {'F': 'FLOAT',
            'L': 'BOOLEAN',
            'I': 'INTEGER',
            'C': 'TEXT',
            'N': 'REAL',
            'M': 'TEXT',
            'D': 'DATE',
            'T': 'DATETIME',
            '0': 'INTEGER'}

def convert_to_csv(file_name):
  with open('{}.csv'.format(file_name), 'w') as f:
    writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(db.field_names)
    count=0
    for rec in db:
      writer.writerow(rec.values())
      bar.update(count)
      count+=1

def convert_to_sql(file_name):
  mysql_user = input('Enter MySQL username: ')
  mysql_pass = getpass.getpass('Enter MySQL password: ')
  conn = MySQLdb.connect(user=mysql_user, password=mysql_pass)
  conn.set_character_set('utf8')
  cur = conn.cursor()
  sql_query = "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;".format(file_name)
  cur.execute(sql_query)
  types = OrderedDict()
  for field in db.fields:
    if field.type == 'M':
      length = '({})'.format(500)
    elif field.type == 'D':
      length = ''
    else:
      length = '({})'.format(field.length)

    types[field.name] = "{}{}".format(TYPE_MAP[field.type], length)

  query = ", ".join('{} {}'.format(k, v) for k, v in types.items())
  sql_query = "USE {}; CREATE TABLE data (id INT NOT NULL AUTO_INCREMENT, {}, PRIMARY KEY(id)) ENGINE = MYISAM;".format(file_name, query)
  cur.execute(sql_query)

  count = 0
  for rec in db:
    keys = ', '.join(list(rec.keys()))
    values = ', '.join("'{}'".format(str(x)) for x in list(rec.values()))
    sql_query = "INSERT INTO data ({}) VALUES ({})".format(keys, values)
    cur.execute(sql_query)
    bar.update(count)
    count+=1


parser = argparse.ArgumentParser()
parser.add_argument('--db', dest='db', required=True,
                    help='Path to the database')
parser.add_argument('--csv', action='store_true',
                    help='Convert dbf file to csv')
parser.add_argument('--sql', action='store_true',
                    help='Convert dbf file to mysql db')
parser.add_argument('--verbose', action='store_true',
                    help='Increase verbosity')
parser.add_argument('--test')
args = parser.parse_args()

log = logging.getLogger(__name__)
if args.verbose:
    log.setLevel(logging.DEBUG)
    level = 'DEBUG'
else:
    log.setLevel(logging.INFO)
    level = 'INFO'
log.addHandler(logger.ConverterStreamHandler())
coloredlogs.install(level=level)

if __name__ == "__main__":
  db_path = args.db
  if db_path:
    db = DBF(db_path, ignorecase=True, ignore_missing_memofile=False)
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    filename = db_path.split('.')[0]
    if args.csv:
      convert_to_csv(filename)
    elif args.sql:
       convert_to_sql(filename)
