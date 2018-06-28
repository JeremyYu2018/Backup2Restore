#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import paramiko
import argparse
import getpass
from backup2restore_util import command_line_args,parse_args,is_backup_or_restore,is_xtrabackup_installed,install_xtrabackup

class BackupDB:
    def __init__(self, host, port, user, passwd, key_filename):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.ssh_client = paramiko.SSHClient().set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(host, 5022, getpass.getuser(), key_filename=key_filename)

    def sftp_exec_command(self):
        sftp_in, sftp_out, sftp_err = self.ssh_client.exec_command("hostname")
        return sftp_out.read()

    def backup(self):
        self.xtrabackup_dumpdb()

    def xtrabackup_dumpdb(self):
        if not is_xtrabackup_installed():
            install_xtrabackup()

class RestoreDB:
    pass

if  __name__ == '__main__':
    if not is_backup_or_restore(sys.argv[1]):
        print("first arg must begin with '--backup' or '--restore'")
        sys.exit()
    args = command_line_args(sys.argv[2:])
    config = {"host": args.host, "port": args.port, "key_filename": "/home/"+getpass.getuser()+"/.ssh/id_rsa"}
    if "--backup" == sys.argv[1]:
        backupDB = BackupDB(config["host"], config["port"], config["key_filename"], backup_dir=args.backup_dir)
        backupDB.backup()
    else:
        restoreDB = RestoreDB(config["host"], config["port"],)
        restoreDB.restore()
