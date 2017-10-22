#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-12 13:29:06
# @File : XiaoquAreaSpider.py
# @Software : PyCharm
import json
import random
import re
import sqlite3
import MySQLdb
import bs4
import requests
from bs4 import BeautifulSoup
import urlparse

hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

class spider_area(object):
    '''
    爬取大区域链接：大众模板
    '''
    def spider_url_area_normal(self,url,city,code):
        try:
            # proxies = {"https": "http://115.216.230.209:3936"}
            # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)], proxies=proxies)
            r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
            #print r.text
        except Exception, e:
            print e.message
            return
        if r.text is not None:
            soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
            for child in soup.find(class_="position").children:
                try:
                    if type(child.find("div")) == bs4.element.Tag:
                        for link in child.find("div").find_all("a"):
                            # 获取大区域链接
                            area = link.string
                            area_link = link['href']
                            area_name = area_link.split("/")[2]
                            new_url = urlparse.urljoin(url, area_link)
                            table_name = "lianjia_%s_%s_position_info"%(code,area_name)
                            # 将大区域信息插入mysql数据库
                            print new_url,city,area,table_name
                            spider_area.create_city_link_db(new_url,city,area,table_name)
                except Exception,e:
                    print e.message

    '''
    爬取大区域链接：上海／苏州定制版
    '''
    def spider_url_area_special(self,url,city,code):
        try:
            # proxies = {"https": "http://115.216.230.209:3936"}
            # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)], proxies=proxies)
            r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
            #print r.text
        except Exception, e:
            print e.message
            return
        if r.text is not None:
            soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
            if len(soup.find("div",class_="option-list gio_district").find_all("a")) > 0:
                for link in soup.find("div",class_="option-list gio_district").find_all("a"):
                    class_ = link['class'][0]
                    if class_ == "":

                        area = link.string
                        area_link = link['href']
                        new_url = urlparse.urljoin(url, area_link)
                        area_name = area_link.split("/")[2]
                        table_name = "lianjia_%s_%s_position_info" % (code, area_name)
                        print new_url,city,area,table_name
                        spider_area.create_city_link_db(new_url, city, area,table_name)

    def spider_url_area_ll(self):
        # 读取城市配置
        citys = spider_area.read_city_json()
        for city in citys:
            '''
            city['city']城市中文名称
            city['code']城市英文简写
            '''
            url = "https://%s.lianjia.com/xiaoqu/rs/" % city['code']
            # 爬取大区链接：上海／苏州采用不同模板
            if city['code'] == "sh":
                # print city['city'],city['code'],url
                spider_area.spider_url_area_special(url, city['city'],city['code'])
            elif city['code'] == "su":
                url = "http://%s.lianjia.com/xiaoqu/rs/" % city['code']
                # print city['city'], city['code'], url
                spider_area.spider_url_area_special(url, city['city'],city['code'])
            else:
                spider_area.spider_url_area_normal(url, city['city'],city['code'])

    def spider_list_url(self,url):
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0"
        # proxies = {"https": "http://117.95.31.73:2671"}
        # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)],proxies=proxies)
        r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
        soup = BeautifulSoup(r.text,'lxml',from_encoding='utf-8')
        # 爬取分页数据列表
        if soup.find(class_="page-box house-lst-page-box") is not None:
            page_data = soup.find_all("div",class_="page-box house-lst-page-box")[0]['page-data']
            page_url = soup.find_all("div",class_="page-box house-lst-page-box")[0]['page-url']

            if int(json.loads(page_data).get("totalPage")) > 0:
                for i in range(int(json.loads(page_data).get("totalPage"))):
                    try:
                        new_url = urlparse.urljoin(url,re.sub(r"{page}",str(i+1),page_url))
                        # 爬取房源信息详情页
                        # r = requests.get(new_url, headers=headers,proxies=proxies)
                        r = requests.get(new_url,headers=hds[random.randint(0, len(hds) - 1)])
                        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
                        # print soup.prettify()
                        if len(soup.find_all(class_="listContent")) > 0:
                            for each in soup.find_all(class_="listContent")[0].find_all("li"):
                                try:
                                    if len(each.find_all("div", class_="info")) == 1 :
                                        title = each.find_all("div", class_="info")[0].find(class_="title").a.string
                                        detail_url = each.find_all("div", class_="info")[0].a['href']
                                    if each.find_all(class_="tagList")[0].span is not None:
                                        tag_list = each.find(class_="tagList").span.string
                                    else:
                                        tag_list = ""
                                    if len(each.find_all("div", class_="xiaoquListItemPrice")) == 1:
                                        price = each.find_all("div", class_="xiaoquListItemPrice")[0].find(class_="totalPrice").span.string
                                    print title,price,detail_url,tag_list

                                    # 插入空值依次为经度、纬度，0位是否爬取经纬度标识
                                    content = (title,price,'','',detail_url,tag_list,0)
                                    if len(content) == 7:
                                        try:
                                            conn = sqlite3.connect('E:/tuotuo/dbdata/gz_lianjia_xiaoqu.db')
                                        # 创建sqlite数据库
                                            cur = conn.cursor()
                                            cur.execute(' INSERT INTO gz_lianjia_xiaoqu_tianhe VALUES(?,?,?,?,?,?,?)',content)
                                            conn.commit()
                                            conn.close()
                                        except Exception, e:
                                            print e
                                except Exception,e:
                                    print e

                    except Exception,e:
                        print e

    def create_linkdb(self):
        conn = sqlite3.connect('E:/tuotuo/dbdata/gz_lianjia_xiaoqu.db')
        # 创建sqlite数据库
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS gz_lianjia_xiaoqu_tianhe('
                    'title VARCHAR(100),'
                    'price VARCHAR(20),'
                    'longitude VARCHAR(50),'
                    'latitude VARCHAR(50),'
                    'detail_url VARCHAR(50),'
                    'tag_list VARCHAR(100),flag integer)')
        conn.close()

    # 解析经纬度
    def spider_position_info(self, url):
        user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0"
        headers = {'User-Agent': user_agent}
        proxies = {"https": "125.67.70.46:2132"}
        r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)], proxies=proxies)
        # r = requests.get(url, headers=headers)
        # print r.text
        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
        details = soup.find_all(text=re.compile("resblockPosition"))
        if len(details) > 0:
            pattern = re.compile(r"(resblockPosition:')(\d+.\d+)(,)(\d+.\d+)'")
            longitude = re.search(pattern, details[0]).group(2)
            latitude = re.search(pattern, details[0]).group(4)
            return longitude, latitude
            # 更新数据库经纬度信息

    def update_db(self, tablename):
        conn = sqlite3.connect('E:/tuotuo/dbdata/gz_lianjia_link.db')
        # 创建sqlite数据库
        cur = conn.cursor()
        cur.execute(' SELECT * FROM (%s)' % tablename)
        res = cur.fetchall()
        try:
            i = 0
            for line in res:
                try:
                    if line[6] == 0:
                        url = line[3]
                        location = spider_area.spider_position_info(url)
                        if len(location) == 2:
                            longitude = location[0]
                            latitude = location[1]
                            print line[0], url, longitude, latitude
                            # 刷新数据库经纬度信息
                            cur.execute(
                                'UPDATE gz_lianjia_link_panyu SET longitude = ?,latitude = ?,flag = 1 WHERE flag == 0 AND detail_url = ?',
                                (longitude, latitude, url))
                            conn.commit()
                except Exception, e:
                    print e.message
        except Exception, e:
            print e.message
        finally:
            conn.close()

    def read_city_json(self):
        with open('city.json') as json_file:
            datas = json.load(json_file)
            return datas

    def create_city_link_db(self,url,city,area,table_name):
        #print url, city, area
        conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='spider',port=3306,charset='utf8')
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS quanguo_xiaoqu_root_url(city VARCHAR(20) character set utf8,area VARCHAR(20) character set utf8 ,url VARCHAR(50),data_table VARCHAR(50),flag INT DEFAULT 0)')
        data = "'%s','%s','%s','%s'"%(city,area,url,table_name)
        #print data
        cur.execute('insert into quanguo_xiaoqu_root_url (city,area,url,data_table) VALUES (%s)'%data);
        conn.commit()
        conn.close()

if __name__ == '__main__':
    spider_area = spider_area()
    # 爬取所有大区域链接
    spider_area.spider_url_area_ll()
    # 创建数据库
    #spider_area.create_linkdb()
    # 爬取住房链接
    #spider_area.spider_list_url('https://gz.lianjia.com/xiaoqu/tianhe/')
