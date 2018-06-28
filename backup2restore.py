#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import paramiko
from backup2restore_util import command_line_args,parse_args,is_backup_or_restore

class BackupDB:
    def __init__(self, host, port, user, passwd, key_filename):

        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.ssh_client = paramiko.SSHClient().set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(host, 5022, "db_sysop", key_filename=key_filename)

    def show(self):
        show_in, show_out, show_err = self.ssh_client.exec_command("hostname")
        print(show_out.read())

if  __name__ == '__main__':
    if not is_backup_or_restore(sys.argv[1]):
        print("first arg must begin with '--backup' or '--restore'")
        sys.exit()
    else:
        args = command_line_args(sys.argv[2:])
        conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
        backupDB = BackupDB(conn_setting["host"],conn_setting["port"],conn_setting["user"],conn_setting["passwd"])
        backupDB.show()

Backup2Restore