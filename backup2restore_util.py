#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import MySQLdb

manage_host = "192.168.10.211"
manage_port = 3310
username = "bkuser"
password = "bkuser"


class Dbapi(object):
    def __init__(self, host, user, password, port, database=None):
        self.conn = MySQLdb.connect(host=host, user=user, passwd=password, port=int(port),db=database, autocommit=1, charset='utf8')
        if database:
            self.conn.select_db(database)
        self.cur = self.conn.cursor()

    def conn_query(self, sql, *param):
        try:
            rel = self.cur.execute(sql, *param)
            result = self.cur.fetchall()
        except Exception as e:
            result = e
        return result

    def conn_dml(self, sql, *param):
        try:
            rel = self.cur.execute(sql, *param)
            if rel:
                pass
            else:
                return rel
        except Exception as e:
            return e

    def dml_commit(self):
        self.conn.commit()

    def dml_rollback(self):
        self.conn.rollback()

    def close(self):
        self.cur.close()


def parse_args():
    """parse args for backup2restore"""
    parser = argparse.ArgumentParser(description="backup and restore with python,first arg must begin with '--backup' or '--restore'", add_help=False)
    parser.add_argument('--backup',dest='backup', type=str, help='choose this to backup remote database',default=None)
    parser.add_argument('--restore',dest='restore', type=str, help='choose this restore local file to remote server',default=None)
    parser.add_argument('-h', '--host',dest='host', type=str, help='Host the MySQL database server located',default=None)
    parser.add_argument('-P', '--port', dest='port', type=int, help='MySQL port to use', default=3306)
    parser.add_argument('-u', '--user', dest='user', type=str, help='MySQL Username to log in as', default='root')
    parser.add_argument('-p', '--password', dest='password', type=str, help='MySQL Password to use', default='')
    parser.add_argument('-b', '--backup_dir', dest='backup_dir', type=str, help='the directory to store backup', default='/data01/backup')
    parser.add_argument('-B', '--backup_file', dest='backup_file', type=str, help='the file to restore', default=None)
    parser.add_argument('-s', '--software', type=str, help='MySQL software', default=None)
    parser.add_argument('--basedir', type=str, help='the directory to store MySQL software', default="/opt/mariadb")
    parser.add_argument('-d', '--datadir', type=str, help='the directory to store MySQL data', default="/data01/data")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)
    return parser

def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.backup and args.restore:
        raise ValueError('Only one of backup or restore can be True')
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if get_fs_freespace(args.backup_dir) < 10000:
        raise ValueError('target backup directory too small to dump: backup_dir')
    return args

def get_fs_freespace(dirname):
    try:
        fp = os.popen("df -Pk %s" % dirname)
        rs = fp.readlines()
        fp.close()
    except:
        w_log("get filesystem space fail")
    return int(rs[1].split()[3])

# 发邮件
def sendmsg(info):
    print(info)

# 写日志
def w_log(info):
    filemaxsize = 2 * 1024 * 1024  # 2M
    try:
        filename = os.getcwd() + '/' + 'backup.log'
        filesize = os.path.getsize(filename)
    except os.error:
        filesize = 0
    if filesize > filemaxsize:
        filedst = filename.replace('.log', '.old')
        if os.path.exists(filedst):
            os.remove(filedst)
        if filedst != filename:
            os.rename(filename, filedst)
        mode = 'w'
    else:
        mode = 'a'
    try:
        fhandle = open(filename, mode)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        fhandle.write(timestamp + ':  ' + info + '\n')
    except IOError:
        pass

def insert_backup_status(host, port, dumpfilename, filesize, starttime, endtime, spendtime):
    con = Dbapi(host=manage_host, port=manage_port, user=username, password=password, database='dbsync')
    sql = r"insert into backup_status(db_server,db_server_port,backup_server,backup_filename,backup_size,backup_starttime,backup_stoptime,backup_duration,bk_ok) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    # print(sql)
    # print(host, port, manage_host, dumpfilename,filesize,starttime,endtime,spendtime,1)
    con = con.conn_dml(sql, (host, port, manage_host, dumpfilename, filesize, starttime, endtime, spendtime, 1))
    print(con)

def check_port():
    """check port if used"""
    pass

def is_dir_exist():
    """check basedir if used"""
    pass


def generate_server_id():
    pass


def is_backup_or_restore(arg):
     if not arg in ["--backup", "--restore"]:
        return False
     return True



