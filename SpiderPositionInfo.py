#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-12 16:25:10
# @File : SpiderPositionInfo.py
# @Software : PyCharm
# 爬取小区经纬度信息：查询全国小区区域链接表：quanguo_xiaoqu_root_url中flag为1的行中data_table字段，获取该字段后对该字段中记录的表格进行经纬度爬取
import logging
import random
import re

import requests
from bs4 import BeautifulSoup

from util import Util

hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}, \
     {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'}, \
     {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'}, \
     {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'}, \
     {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'}, \
     {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
     {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
     {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'}, \
     {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
     {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
     {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'}, \
     {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'}, \
     {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]


class spider_position_info(object):
    def spider_position_info(self, url):
        '''
        解析经纬度
        :param url:房产详情页的URL
        :return:
        '''
        r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)])
        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
        details = soup.find_all(text=re.compile("resblockPosition"))
        if len(details) > 0:
            pattern = re.compile(r"(resblockPosition:')(\d+.\d+)(,)(\d+.\d+)'")
            longitude = re.search(pattern, details[0]).group(2)
            latitude = re.search(pattern, details[0]).group(4)
            return longitude, latitude

    def spider_position_info_special(self, url):
        '''
        解析经纬度：为上海、苏州量身定做
        :param url: 房产详情页的URL
        :return:
        '''
        r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
        longitude = soup.find(class_="actshowMap")["xiaoqu"].split(",")[0].split("[")[1]
        latitude = soup.find(class_="actshowMap")["xiaoqu"].split(",")[1]
        return longitude, latitude

    def update_db(self):
        '''
        更新小区详情数据中的经纬度表，查询全国小区大区表，获取小区详情数据表名，更新该表中的经纬度信息
        :return:
        '''
        conn = Util().get_db_conn()
        cur = conn.cursor()
        cur.execute('select data_table from quanguo_xiaoqu_root_url WHERE flag = 1')
        lines = cur.fetchall()
        for line in lines:
            table_name = line[0]
            cur.execute('select city,detail_url from %s WHERE flag = 0'%table_name)
            res = cur.fetchall()
            for line in res:
                try:
                    city = line[0]
                    url = line[1]
                    if city == '上海'.decode('utf-8') or city == '苏州'.decode('utf-8'):
                        location = position.spider_position_info_special(url)
                    else:
                        location = position.spider_position_info(url)
                    if len(location) == 2:
                        longitude = location[0]
                        latitude = location[1]
                        print table_name,longitude,latitude,url
                        cur.execute("update %s set longtitude = '%s',latitude = '%s',flag = 1 WHERE detail_url = '%s'"%(table_name,longitude,latitude,url))
                        conn.commit()
                except Exception,e:
                    logging.exception(e)

if __name__ == '__main__':
    position = spider_position_info()
    position.update_db()
