#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime

def parse_args():
    """parse args for backup2restore"""
    parser = argparse.ArgumentParser(description='backup and restore with python', add_help=False)
    parser.add_argument('-h', '--host',dest='host', type=str, help='Host the MySQL database server located',default=None)
    parser.add_argument('-P', '--port', dest='port', type=int, help='MySQL port to use', default=3306)
    parser.add_argument('-u', '--user', dest='user', type=str, help='MySQL Username to log in as', default='root')
    parser.add_argument('-p', '--password', dest='password', type=str, help='MySQL Password to use', default='')
    parser.add_argument('-b', '--backup_dir', dest='backup_dir', type=str, help='the directory to store backup', default='/data01/backup')
    parser.add_argument('-B', '--backup_file', dest='backup_dir', type=str, help='the file to restore', default=None)
    parser.add_argument('-s', '--software', type=str, help='MySQL software', default=None)
    parser.add_argument('-b', '--basedir', type=str, help='the directory to store MySQL software', default="/opt/mariadb"+)
    parser.add_argument('-d', '--datadir', type=str, help='the directory to store MySQL data', default="/data01/data")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)
    return parser

def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if get_freespace(args.backup_dir) < 10G:
        raise ValueError('target backup directory too small to dump: backup_dir')
    return args


def check_port():
    """check port if used"""
    pass

def is_dir_exist():
    """check basedir if used"""
    pass


def generate_server_id():
    pass


def is_xtrabackup_installed():
    backup_tool = "innobackupex"
    for cmdpath in os.environ['PATH'].split(':'):
        if os.path.isdir(cmdpath) and backup_tool in os.listdir(cmdpath):
            return True
    return False

def install_xtrabackup():


def is_backup_or_restore(arg):
     if not arg in ["--backup", "--restore"]:
        return False
     return True



