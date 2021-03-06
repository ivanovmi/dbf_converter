import argparse
import getpass
import logging
import sys
import yaml

import _mysql_exceptions
import coloredlogs
import dbfread
import MySQLdb
import progressbar


def auth_db(db_host, db_port):
    mysql_user = input('Пользователь MySQL: ')
    mysql_pass = getpass.getpass('Пароль MySQL пользователя'
                                 ' {}: '.format(mysql_user))
    try:
        conn = MySQLdb.connect(user=mysql_user, password=mysql_pass,
                               host=db_host, port=db_port)
    except _mysql_exceptions.OperationalError:
        log.error('Пользователь не прошел авторизацию!')
    conn.set_character_set('utf8')
    cur = conn.cursor()
    return cur


def init_db(db_connection):
    """@todo."""
    log.debug('Создание базы omcsmo')
    sql_query = ("CREATE DATABASE IF NOT EXISTS omcsmo DEFAULT CHARACTER SET"
                 " utf8 DEFAULT COLLATE utf8_general_ci")
    db_connection.execute(sql_query)
    log.debug('Создание таблицы rp')
    sql_query = ("USE omcsmo; CREATE TABLE IF NOT EXISTS rp ("
                 "id int(11) unsigned NOT NULL AUTO_INCREMENT,"
                 "cd_lpu mediumint(7) unsigned NOT NULL,"
                 "ppac date NOT NULL,"
                 "cd_sect smallint(3) unsigned NOT NULL,"
                 "n_s mediumint(5) unsigned NOT NULL,"
                 "nstr_grn mediumint(5) unsigned NOT NULL,"
                 "dspc date NOT NULL,"
                 "dfpc date NOT NULL,"
                 "drpc date NOT NULL,"
                 "fam char(40) NOT NULL,"
                 "nam char(40) NOT NULL,"
                 "snam char(40) NOT NULL,"
                 "sex char(1) NOT NULL,"
                 "cd_mkb char(6) NOT NULL,"
                 "q_haus mediumint(6) unsigned NOT NULL,"
                 "q_recp mediumint(6) unsigned NOT NULL,"
                 "vsl tinyint(2) unsigned NOT NULL,"
                 "isx char(65) NOT NULL,"
                 "sn_pol char(16) NOT NULL,"
                 "nk char(50) NOT NULL,"
                 "vp tinyint(1) unsigned NOT NULL,"
                 "vidpom smallint(4) unsigned NOT NULL,"
                 "kmnt smallint(4) NOT NULL,"
                 "kmnt2 varchar(500) NOT NULL,"
                 "type tinyint(2) unsigned NOT NULL,"
                 "cd_spc mediumint(6) unsigned NOT NULL,"
                 "cna decimal(10,2) NOT NULL,"
                 "sum_u decimal(10,2) NOT NULL,"
                 "q_pos mediumint(6) unsigned NOT NULL,"
                 "q_zsl mediumint(6) unsigned NOT NULL,"
                 "k_age tinyint(1) unsigned NOT NULL,"
                 "vid_tarif tinyint(1) unsigned NOT NULL,"
                 "PRIMARY KEY (id))"
                 " ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;")
    db_connection.execute(sql_query)


def to_sql(db, db_connection, db_name, table_name):
    count = 0
    for rec in db:
        keys = ', '.join(list(rec.keys()))
        values = ', '.join("'{}'".format(str(x)) for x in list(rec.values()))

        sql_query = "INSERT INTO {}.{} ({}) VALUES ({})".format(db_name,
                                                               table_name,
                                                               keys,
                                                               values)
        db_connection.execute(sql_query)
        bar.update(count)
        count += 1


parser = argparse.ArgumentParser()
parser.add_argument('--db', dest='db',  # required=True,
                    help='Path to the database')
parser.add_argument('--db-host', dest='db_host', default='127.0.0.1',
                    help='Адрес MySQL сервера')
parser.add_argument('--db-port', dest='db_port', default=3306,
                    type=int, help='Порт для доступа к MySQL серверу')
parser.add_argument('--init', action='store_true',
                    help='Use this option for db initialization')
parser.add_argument('--verbose', action='store_true',
                    help='Increase verbosity')
parser.add_argument('--config', dest='config', required=True,
                    help='Path to config')
args = parser.parse_args()

log = logging.getLogger(__name__)
if args.verbose:
    log.setLevel(logging.DEBUG)
    level = 'DEBUG'
else:
    log.setLevel(logging.INFO)
    level = 'INFO'
coloredlogs.install(level=level)

if __name__ == "__main__":
    connection = auth_db(args.db_host, args.db_port)

    with open(args.config, 'r') as f:
        cfg = yaml.load(f)
        for db in cfg['db']:
            log.info('Initializing {db_name} db'.format(db_name=db['name']))
            sql_query = (
                "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET"
                " utf8 DEFAULT COLLATE utf8_general_ci".format(db['name']))
            connection.execute(sql_query)
            for table in db['tables']:
                sql_query = "USE {db_name}; ".format(db_name=db['name'])
                log.info('Initializing {table_name} in'
                         ' {db_name}'.format(table_name=table['name'],
                                             db_name=db['name']))
                sql_query += "CREATE TABLE IF NOT EXISTS {table_name} (".format(table_name=table['name'])
                for column in table['columns']:
                    sql_query += "{column_name} " \
                                 "{column_type} " \
                                 "{sign} " \
                                 "{nullable} " \
                                 "{auto_increment},". \
                        format(column_name=column['name'],
                               column_type=column['type'],
                               sign="unsigned" if column.get('unsigned', None) is True else "",
                               nullable="NOT NULL" if column[None] is False else "NULL",
                               auto_increment="AUTO_INCREMENT" if column.get('auto_increment', None) is True else "")
                sql_query += "PRIMARY KEY ({})) ".format(table['primary_key'])
                sql_query += "ENGINE={engine}" \
                             " DEFAULT CHARSET={charset} " \
                             "AUTO_INCREMENT={inc};". \
                    format(engine=table.get('engine', 'MyISAM'),
                           charset=table.get('charset', 'utf8'),
                           inc=table.get('auto_increment', 0))
                connection.execute(sql_query)
                for _file in table['files']:
                    db_file = dbfread.DBF(_file, ignorecase=True,
                                          ignore_missing_memofile=False)
                    bar = progressbar.ProgressBar(
                        max_value=progressbar.UnknownLength)
                    to_sql(db_file, connection, db['name'], table['name'])
    """
    if args.init:
        log.info('Начало инициализации базы данных')
        init_db(connection)
        log.info('База была проинициализирована!')
        sys.exit()
    else:
        db_path = args.db
        if db_path:
            db = dbfread.DBF(db_path, ignorecase=True,
                             ignore_missing_memofile=False)
            bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
            filename = db_path.split('.')[0]
            to_sql(db, connection)
    """