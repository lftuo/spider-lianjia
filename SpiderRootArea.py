#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-23 13:22:33
# @File : SpiderRootArea.py
# @Software : PyCharm
# 生成全国小区区域链接表：如南京、建邺；上海、浦东
import json
import logging
import random
import urlparse

import bs4
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
class spider_root_area(object):

    def spider_url_area_normal(self, url, city, code):
        '''
        取大区域链接：大众模板
        :param url: 城市URL
        :param city: 城市名称
        :param code: 城市网站英文代码
        :return:
        '''

        try:
            # proxies = {"https": "http://115.216.230.209:3936"}
            # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)], proxies=proxies)
            r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
            # print r.text
        except Exception, e:
            print e.message
            return
        if r.text is not None:
            soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
            for child in soup.find(class_="position").children:
                try:
                    if type(child.find("div")) == bs4.element.Tag:
                        for link in child.find("div").find_all("a"):
                            # 获取大区名称
                            area = link.string
                            # 获取大区域链接
                            area_link = link['href']
                            length = len(area_link.split("/"))
                            area_name = area_link.split("/")[length - 2]
                            new_url = urlparse.urljoin(url, area_link)
                            # 拼接生成大区数据存储表名
                            table_name = "lianjia_%s_%s_position_info" % (code, area_name)
                            # 将大区域信息插入mysql数据库
                            print new_url, city, area, table_name
                            # 将数据存储quanguo_xiaoqu_root_url表
                            spider.insert_xiaoqu_root_url(new_url, city, area, table_name)
                except Exception, e:
                    print e.message


    def spider_url_area_special(self, url, city, code):
        '''
        爬取大区域链接：上海／苏州定制版
        :param url: 城市URL
        :param city: 城市名称
        :param code: 城市网站英文代码
        :return:
        '''

        try:
            # proxies = {"https": "http://115.216.230.209:3936"}
            # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)], proxies=proxies)
            r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
            # print r.text
        except Exception, e:
            print e.message
            return
        if r.text is not None:
            soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
            if len(soup.find("div", class_="option-list gio_district").find_all("a")) > 0:
                for link in soup.find("div", class_="option-list gio_district").find_all("a"):
                    class_ = link['class'][0]
                    if class_ == "":
                        # 获取大区名称
                        area = link.string
                        # 获取大区连接
                        area_link = link['href']
                        new_url = urlparse.urljoin(url, area_link)
                        area_name = area_link.split("/")[2]
                        # 拼接生成大区数据存储表名
                        table_name = "lianjia_%s_%s_position_info" % (code, area_name)
                        print new_url, city, area, table_name
                        # 将数据存储quanguo_xiaoqu_root_url表
                        spider.insert_xiaoqu_root_url(new_url, city, area, table_name)

    def spider_url_area_ll(self):
        '''
        爬取可爬地市的所有区域链接，生成区域数据存储表quanguo_xiaoqu_root_url
        :return:
        '''

        try:
            conn = Util().get_db_conn()
            cur = conn.cursor()
            cur.execute('drop table if EXISTS quanguo_xiaoqu_root_url')
            cur.execute(
                'CREATE TABLE quanguo_xiaoqu_root_url(city VARCHAR(20) character set utf8,area VARCHAR(20) character set utf8 ,url VARCHAR(50),data_table VARCHAR(50),flag INT DEFAULT 0)')
            conn.commit()
            conn.close()
        except Exception, e:
            logging.exception(e)
            return

        # 读取城市配置
        citys = spider.read_city_json()
        for city in citys:
            # city['city']城市中文名称
            # city['code']城市英文简写
            url = "https://%s.lianjia.com/xiaoqu/rs/" % city['code']
            # 爬取大区链接：上海／苏州采用不同模板
            if city['code'] == "sh":
                spider.spider_url_area_special(url, city['city'], city['code'])
            elif city['code'] == "su":
                url = "http://%s.lianjia.com/xiaoqu/rs/" % city['code']
                spider.spider_url_area_special(url, city['city'], city['code'])
            else:
                spider.spider_url_area_normal(url, city['city'], city['code'])

    def read_city_json(self):
        '''
        读取爬虫需要爬取得配置文件city.json
        :return:
        '''

        with open('conf/city.json') as json_file:
            datas = json.load(json_file)
            return datas

    def insert_xiaoqu_root_url(self, url, city, area, table_name):
        '''
        :param url: 大区域URL
        :param city:城市名称
        :param area:城市区域名称
        :param table_name:数据存储表明
        :return:
        '''

        conn = Util().get_db_conn()
        cur = conn.cursor()
        data = "'%s','%s','%s','%s'" % (city, area, url, table_name)
        cur.execute('insert into quanguo_xiaoqu_root_url (city,area,url,data_table) VALUES (%s)' % data);
        conn.commit()
        conn.close()


if __name__ == '__main__':
    spider = spider_root_area()
    spider.spider_url_area_ll()