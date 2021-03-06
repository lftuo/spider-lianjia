#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2017-10-12 13:29:06
# @File : SpiderXiaoquLink.py
# @Software : PyCharm
# 爬取小区详情链接，查询国小区区域链接表：quanguo_xiaoqu_root_url中flag为0的URL进行爬取，爬取完成后flag置1
from __future__ import division

import json
import random
import re
import urlparse

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

class spider_area(object):

    def spider_xiiaoqu_root_url(self):
        '''
        查询全国小区区域链接的数据，表名：quanguo_xiaoqu_root_url，按区域存储，每爬完一个区域，flag置1
        :return:
        '''

        conn = Util().get_db_conn()
        cur = conn.cursor()
        cur.execute('select * from quanguo_xiaoqu_root_url')
        lines = cur.fetchall()
        for line in lines:
            # 大区所在城市：如南京市
            city = line[0]
            # 大区所在城市的取：如建邺区
            area = line[1]
            # 大区链接：如建邺区链接：https://nj.lianjia.com/xiaoqu/jianye/
            url = line[2]
            # 小区数据存储表名：如建邺区数据存储表名lianjia_nj_jianye_position_info
            table_name = line[3]
            # 是否已爬取标志位：0为未爬取，1为已爬取
            flag = line[4]
            print line[0],line[1],line[2],line[3]
            if flag == 0:
                if city == '上海'.decode('utf-8') or city == '苏州'.decode('utf-8'):
                    spider_area.spider_list_url_special(city,area,url,table_name)
                else:
                    spider_area.spider_list_url(city,area,url,table_name)
        conn.close()

    def spider_list_url(self,city,area,url,table_name):
        '''
        获取小区详细房产信息
        :param city: 小区所在城市
        :param area: 小区所在区
        :param url:小区所在大区URL
        :param table_name:数据存储表名
        :return:
        '''

        # proxies = {"https": "http://117.95.31.73:2671"}
        # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)],proxies=proxies)
        r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
        soup = BeautifulSoup(r.text,'lxml',from_encoding='utf-8')
        conn = Util().get_db_conn()
        cur = conn.cursor()
        cur.execute('drop table if EXISTS %s'%table_name)
        cur.execute('CREATE TABLE %s(area_name VARCHAR(100) character set utf8,price VARCHAR(20) DEFAULT NULL ,longtitude VARCHAR(50) DEFAULT NULL ,latitude VARCHAR(50) DEFAULT NULL ,city VARCHAR(20) character set utf8 ,area VARCHAR(20) character set utf8 ,tag_list VARCHAR(100) character set utf8 DEFAULT NULL ,detail_url VARCHAR(50) DEFAULT NULL ,flag INT DEFAULT 0)'%table_name)

        # 爬取分页数据列表
        flag = True
        try:
            if soup.find(class_="page-box house-lst-page-box") is not None:
                page_data = soup.find_all("div",class_="page-box house-lst-page-box")[0]['page-data']
                page_url = soup.find_all("div",class_="page-box house-lst-page-box")[0]['page-url']
                if int(json.loads(page_data).get("totalPage")) > 0:
                    # 循环获取每页数据，有单页无法获取，则向上异常抛出
                    for i in range(int(json.loads(page_data).get("totalPage"))):
                        new_url = urlparse.urljoin(url,re.sub(r"{page}",str(i+1),page_url))
                        # 爬取房源信息详情页
                        # r = requests.get(new_url, headers=headers,proxies=proxies)
                        r = requests.get(new_url,headers=hds[random.randint(0, len(hds) - 1)])
                        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
                        # print soup.prettify()
                        if len(soup.find_all(class_="listContent")) > 0:
                            # 循环获取单条房产信息，单条房产数据获取异常则捕获，进行下一个
                            for each in soup.find_all(class_="listContent")[0].find_all("li"):
                                try:
                                    if len(each.find_all("div", class_="info")) == 1 :
                                        area_name = each.find_all("div", class_="info")[0].find(class_="title").a.string
                                        detail_url = each.find_all("div", class_="info")[0].a['href']
                                    if each.find_all(class_="tagList")[0].span is not None:
                                        tag_list = each.find(class_="tagList").span.string
                                    else:
                                        tag_list = ""
                                    if len(each.find_all("div", class_="xiaoquListItemPrice")) == 1:
                                        price = each.find_all("div", class_="xiaoquListItemPrice")[0].find(class_="totalPrice").span.string
                                    print area_name,price,detail_url,tag_list

                                    # 数据存入该区对应数据库
                                    content = "'%s','%s','%s','%s','%s','%s'" % (area_name,price,city,area,detail_url,tag_list)
                                    if area_name is not None and price is not None and detail_url is not None and tag_list is not None:
                                        cur.execute(' INSERT INTO %s(area_name,price,city,area,detail_url,tag_list) VALUES(%s)'%(table_name,content))
                                        conn.commit()
                                except Exception,e:
                                    print e.message
        except Exception,e:
            # 非正常退出程序，则设置标志位flag为FALSE，下次重新抽取
            flag = False
            print e.message
        finally:
            # 正常结束程序，则更新root表标志位1
            if flag is True:
                cur.execute("UPDATE quanguo_xiaoqu_root_url SET flag=1 WHERE data_table = '%s'" % table_name)
            conn.commit()
            conn.close()


    def spider_list_url_special(self,city,area,url,table_name):
        '''
        上海/苏州定制版:爬取房产详细链接
        :param city: 小区所在城市
        :param area: 小区所在区
        :param url: 小区所在大区URL
        :param table_name: 数据存储表名
        :return:
        '''

        # proxies = {"https": "http://117.95.31.73:2671"}
        # r = requests.get(url, headers=hds[random.randint(0,len(hds)-1)],proxies=proxies)
        r = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
        soup = BeautifulSoup(r.text,'lxml',from_encoding='utf-8')
        conn = Util().get_db_conn()
        cur = conn.cursor()
        cur.execute('drop table if EXISTS %s'%table_name)
        cur.execute('CREATE TABLE %s(area_name VARCHAR(100) character set utf8,price VARCHAR(20) DEFAULT NULL ,longtitude VARCHAR(50) DEFAULT NULL ,latitude VARCHAR(50) DEFAULT NULL ,city VARCHAR(100) character set utf8 ,area VARCHAR(100) character set utf8 ,tag_list VARCHAR(100) character set utf8 DEFAULT NULL ,detail_url VARCHAR(100) DEFAULT NULL ,flag INT DEFAULT 0)'%table_name)

        # 爬取分页数据列表
        flag = True
        try:
            # 遍历大区下小区
            for child in soup.find(class_="option-list sub-option-list gio_plate").find_all("a"):
                if child["class"][0] != "on":
                    child_url = urlparse.urljoin(url,child['href'])
                    #print child_url
                    r = requests.get(child_url, headers=hds[random.randint(0, len(hds) - 1)])
                    soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
                    # 通过观察页面，爬取总页数比较困难，可以爬取总房产数，与每页显示房产数求余，算出总页数；通过观察发现，每页显示总页数为20
                    # 找到的房源总数
                    house_num = int(soup.find(class_="list-head clear").find_all("h2")[0].find_all("span")[0].string)
                    if house_num % 20 == 0:
                        page_num = int(house_num/20)
                    else:
                        page_num = int((house_num/20)+1)
                    # print house_num,str(house_num/20),page_num
                    # 拼接单个区域下的子页链接：如上海市-黄埔区-北蔡[1-12]页链接
                    for i in range(page_num):
                        parent_url = child_url[0:len(child_url)-2]
                        link = parent_url+"d%srs"%(i+1)
                        r = requests.get(link, headers=hds[random.randint(0, len(hds) - 1)])
                        soup = BeautifulSoup(r.text, 'lxml', from_encoding='utf-8')
                        if len(soup.find_all("div",class_="info-panel")) > 0:
                            for detail in soup.find_all("div",class_="info-panel"):
                                try:
                                    # 获取小区名称
                                    area_name = detail.find("h2").find("a").string
                                    # 获取小区详细URL
                                    fangchan_url = detail.find("h2").find("a")['href']
                                    detail_url = urlparse.urljoin(link,fangchan_url)
                                    # 获取小区均价
                                    price = detail.find(class_="price").find("span").string
                                    # 获取小区特点
                                    if detail.find(class_='fang-subway-ex') is not None:
                                        tag_list = detail.find(class_='fang-subway-ex').find("span").string
                                    else:
                                        tag_list = ""
                                    print area_name.strip(),price.strip(),detail_url.strip(),tag_list.strip()
                                    # 数据存入该区对应数据库
                                    content = "'%s','%s','%s','%s','%s','%s'" % (area_name.strip(),price.strip(),city,area,detail_url.strip(),tag_list.strip())
                                    if area_name is not None and price is not None and detail_url is not None and tag_list is not None:
                                        cur.execute(
                                            ' INSERT INTO %s(area_name,price,city,area,detail_url,tag_list) VALUES(%s)' % (
                                                table_name, content))
                                        conn.commit()
                                except Exception,e:
                                    print e

        except Exception,e:
            # 非正常退出程序，则设置标志位flag为FALSE，下次重新抽取
            flag = False
            print e.message
        finally:
            # 正常结束程序，则更新root表标志位1
            if flag is True:
                cur.execute("UPDATE quanguo_xiaoqu_root_url SET flag=1 WHERE data_table = '%s'" % table_name)
            conn.commit()
            conn.close()

if __name__ == '__main__':
    spider_area = spider_area()
    spider_area.spider_xiiaoqu_root_url()
