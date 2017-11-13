#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-23 14:08:49
# @File : MergeXiaoquLinkTab.py
# @Software : PyCharm
# 合并所有的小区表：查询全国小区区域链接表：quanguo_xiaoqu_root_url中flag为1的行中data_table字段,
#               合并data_table表单中的数据插入总表quanguo_xiaoqu_position_info
from util import Util


class merge_tables(object):
    def merge(self):
        conn = Util().get_db_conn()
        cur = conn.cursor()
        # 执行此函数一次，进行一次合并
        cur.execute('drop table if EXISTS quanguo_xiaoqu_position_info')
        # 创建总表
        cur.execute(
            'CREATE TABLE quanguo_xiaoqu_position_info(area_name VARCHAR(100) character set utf8,price VARCHAR(20) DEFAULT NULL ,longtitude VARCHAR(50) DEFAULT NULL ,latitude VARCHAR(50) DEFAULT NULL ,city VARCHAR(20) character set utf8 ,area VARCHAR(20) character set utf8 ,tag_list VARCHAR(100) character set utf8 DEFAULT NULL ,detail_url TEXT ,flag INT DEFAULT 0)')
        conn.commit()
        cur.execute('select data_table from quanguo_xiaoqu_root_url WHERE flag = 1')
        res = cur.fetchall()
        for line in res:
            table_name = line[0]
            print table_name
            cur.execute('insert into quanguo_xiaoqu_position_info SELECT * FROM %s'%table_name)
            conn.commit()
        conn.close()

if __name__ == '__main__':
    merge = merge_tables()
    merge.merge()