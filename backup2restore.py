#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import paramiko
import argparse
import getpass
import time
from backup2restore_util import command_line_args,parse_args,is_backup_or_restore,sendmsg,w_log,insert_backup_status

# 管理服务器ip和port



class BackupDB:
    def __init__(self, host, port, user, passwd, key_filename, backup_dir):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.g_backupdir = backup_dir
        self.ssh_client= paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(host, 5022, getpass.getuser(), key_filename=key_filename)

    def sftp_exec_command(self, command):
        sftp_in, sftp_out, sftp_err = self.ssh_client.exec_command(command,get_pty=True)
        return sftp_out.read()

    def check_mycnf(self):
        try:
            sftp = self.ssh_client.open_sftp()
            sftp.stat("/etc/my.cnf")
            sendmsg("/etc/my.cnf in remote server %s exists," %  self.host)
            w_log("/etc/my.cnf in remote server %s exists," %  self.host)
            self.sftp_exec_command("sudo mv /etc/my.cnf /etc/my.cnf.bak")
        except IOError:
            pass
        finally:
            sftp.close()

    def backup(self):
        self.check_mycnf()
        dumpfilename, filesize, starttime, endtime, spendtime = self.xtrabackup_dumpdb(self.host, self.port, self.user, self.passwd)
        insert_backup_status(self.host, self.port, dumpfilename, filesize, starttime, endtime, spendtime)

    def is_xtrabacup_installed(self):
        decidestr = r"  && echo 1 || echo 0"
        try:
            check_innobackupex_out = self.sftp_exec_command("innobackupex -v" + decidestr)
            if int(check_innobackupex_out.split("\r\n")[-2]):
                sendmsg("xtrabackup exists already software in remote server %s pass" % self.host)
                w_log("xtrabackup exists already software in remote server %s " % self.host)
                return True
            else:
                sendmsg("xtrabackup software not installed in remote server %s " % self.host)
                w_log("xtrabackup software not installed in remote server %s " % self.host)
                sendmsg(check_innobackupex_out)
                w_log(check_innobackupex_out)
                return False
                sys.exit()
        except paramiko.SSHException:
            print("SSHException Error")
            sys.exit(1)

    def install_xtrabackup(self):
        decidestr = r"  && echo 1 || echo 0"
        try:
            install_xtrabackup_str = r"curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup | sudo bash &&yum install -y percona-xtrabackup-24"
            install_xtrabackup_out = self.sftp_exec_command('install_xtrabackup_str' + decidestr)
            if int(install_xtrabackup_out.split("\r\n")[-2]):
                sendmsg("install xtrabackup backup tools in remote server %s  success" % self.host)
                w_log("install xtrabackup backup tools in remote server %s  success" % self.host)
            else:
                sendmsg("install xtrabackup backup tools in remote server %s  failed" % self.host)
                w_log("install xtrabackup backup tools in remote server %s  failed" % self.host)
                sendmsg(install_xtrabackup_out)
                w_log(install_xtrabackup_out)
                sys.exit()
        except paramiko.SSHException:
            print("SSHException Error")
            sys.exit(1)

    def xtrabackup_dumpdb(self, hostip, dbport, dbuser, dbpassword):
        decidestr = r"  && echo 1 || echo 0"
        if not self.is_xtrabacup_installed():
            self.install_xtrabackup()
        backupdir = os.path.join(self.g_backupdir, hostip)
        dumpfilename = hostip + '_' + str(dbport) + '_' + 'full' + '_' + datetime.datetime.strftime(datetime.date.today(),'%Y%m%d') + '.tar.gz'
        filestr = os.path.join(backupdir, dumpfilename)
        cmdstr = "sudo /usr/bin/innobackupex --host=" + hostip + " --user=" + dbuser + " --password=" + str(dbpassword) + " --port=" + str(dbport) + " --stream=tar /tmp/ | gzip | ssh db_sysop@192.168.10.211 -p 5022 \" cat - > " + filestr + "\""
        if not os.path.exists(backupdir):
            if os.system("mkdir -p " + backupdir):
                sendmsg("mdkir directory %s failed" % backupdir)
                w_log("mdkir directory %s failed" % backupdir)
                sys.exit()
            else:
                sendmsg("mdkir directory %s success" % backupdir)
                w_log("mdkir directory %s success" % backupdir)
        if os.path.exists(filestr):
            sendmsg("the backup  exist, no need to backup again")
            w_log("the backup  exist, no need to backup again")
            sys.exit()
        starttime = int(time.time())
        sendmsg("begin dumping database")
        w_log("begin dumping database")
        try:
            dumpdb_out = self.sftp_exec_command(cmdstr + decidestr)
            print(cmdstr+decidestr)
            if int(dumpdb_out.split("\r\n")[-2]):
                sendmsg("xtrabackup dump database in remote server %s  success" % self.host)
                w_log("xtrabackup dump database tools in remote server %s  success" % self.host)
                sendmsg(dumpdb_out)
            else:
                sendmsg("xtrabackup dump database in remote server %s  failed" % self.host)
                sendmsg("xtrabackup dump database in remote server %s  failed" % self.host)
                sendmsg(dumpdb_out)
                w_log(dumpdb_out)
                sys.exit()
        except paramiko.SSHException:
            print("SSHException Error")
            sys.exit(1)
        endtime = int(time.time())
        spendtime = endtime - starttime
        start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime))
        end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))
        return filestr, os.path.getsize(filestr), start, end, spendtime

class RestoreDB:
    pass

    def restore(self):
        pass

if  __name__ == '__main__':
    args = command_line_args(sys.argv[2:])
    if len(sys.argv[0:]) == 1:
        args.print_help()
        sys.exit()
    if not is_backup_or_restore(sys.argv[1]):
        print("first arg must begin with '--backup' or '--restore'")
        sys.exit()
    config = {"host": args.host, "port": args.port, "key_filename": "/home/"+getpass.getuser()+"/.ssh/id_rsa"}
    if "--backup" == sys.argv[1]:
        backupDB = BackupDB(host=config["host"], port=config["port"], user=args.user, passwd=args.password, key_filename=config["key_filename"], backup_dir=args.backup_dir)
        backupDB.backup()
    else:
        restoreDB = RestoreDB(config["host"], config["port"],)
        restoreDB.restore()

