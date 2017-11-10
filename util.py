#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-23 14:08:49
# @File : MergeXiaoquLinkTab.py
# @Software : PyCharm
# 创建工具类
import ConfigParser

import MySQLdb

class Util(object):

    def read_config(self):
        config = ConfigParser.ConfigParser()
        config.read("conf/config.ini")
        return config


    '''
    获取MySQL数据库连接
    '''
    def get_db_conn(self):
        config = self.read_config()
        host_name = config.get("MySQL","MYSQL_HOST")
        db_name = config.get("MySQL","MYSQL_DBNAME")
        user_name = config.get("MySQL","MYSQL_USER")
        password = config.get("MySQL","MYSQL_PASSWD")
        port = int(config.get("MySQL","PORT"))
        charset = config.get("MySQL","CHARSET")
        conn = MySQLdb.connect(host=host_name, user=user_name, passwd=password, db=db_name, port=port, charset=charset)
        return conn


if __name__ == '__main__':
    print Util().get_db_conn()
