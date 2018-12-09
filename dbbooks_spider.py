# -*- coding: utf-8 -*-
import threading
import sys
import hashlib
import os
import re
import random
import logging
import logging.config
from logging.handlers import RotatingFileHandler
import pymysql
import requests
from lxml import etree

reload(sys)
sys.setdefaultencoding('utf-8')

USER_AGENT_LIST = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]

KEYLIST = [
    'bid', 'tag1_name', 'tag2_name', 'tag2_url', 'foreign_author', 'chinese_author', 'publisher', 'publish_date',
    'price','book_url', 'book_name', 'star_num', 'rating_nums', 'comment_num', 'introduction'
]

MYSQL_CONFIG = {
    'TEST_ENV': {
        'HOST': '127.0.0.1',
        'PORT': 3306,
        'USER': 'root',
        'PASSWORD': 'wlpepsi3590010732',
        'DB': 'test1',
        'CHARSET': 'utf8'},
    'PRODUCT_ENV': {

    }
}


class MyLogger(object):
    def __init__(self, log_path=None, log_name="doubanbooks.log"):
        self.log_path = log_path or './logs/'
        if not os.path.exists(self.log_path):
            try:
                os.mkdir(self.log_path)
            except:
                raise Exception('log_path:%s you offered looks wrong！please check!!' % self.log_path)
        self.log_name = log_name

    @property
    def logger_instance(self):
        return self._get_logger()

    def _get_logger(self):
        logger = logging.getLogger(self.log_name[:-4])
        formatter = logging.Formatter("%(asctime)s | %(filename)s[line:%(lineno)d] | %(levelname)s | %(message)s")

        # 文件handler
        file_handler = RotatingFileHandler(self.log_path + self.log_name, maxBytes=5 * 1024 * 1024, backupCount=2,
                                           encoding="utf8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        logger.setLevel(logging.DEBUG)
        return logger


class Dbpool(object):
    def __init__(self, ):
        self.pool = {}
        self.host = MYSQL_CONFIG['TEST_ENV']['HOST']
        self.port = MYSQL_CONFIG['TEST_ENV']['PORT']
        self.username = MYSQL_CONFIG['TEST_ENV']['USER']
        self.pwd = MYSQL_CONFIG['TEST_ENV']['PASSWORD']
        self.dbname = MYSQL_CONFIG['TEST_ENV']['DB']
        self.charset = MYSQL_CONFIG['TEST_ENV']['CHARSET']

    def get_instance(self, ):
        name = threading.current_thread().name
        if name not in self.pool:
            conn = pymysql.connect(host=self.host, port=self.port, user=self.username, password=self.pwd,
                                   db=self.dbname, charset=self.charset)
            self.pool[name] = conn
        return self.pool[name]


class Pipeline(object):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__file__)
        self.con = Dbpool().get_instance()
        self.cursor = self.con.cursor()
        self.table = 'douban_books_info'
        self.db = MYSQL_CONFIG['TEST_ENV']['DB']
        self._create_table()

    def to_mysql(self, item):
        tag1_name=item['tag1_name']
        tag2_name=item['tag2_name']
        book_name=item['book_name']
        select_sql='select * from {} where tag1_name="{}" and tag2_name="{}" and book_name="{}"'.format(self.table,tag1_name,tag2_name,book_name)
        try:
            result=self.cursor.execute(select_sql)
            if not result:
                keys = ','.join([key for key in KEYLIST])
                values = ','.join(['"{}"'.format(item[key].encode('utf-8')) for key in KEYLIST])
                sql = 'insert into {} ({}) value({})'.format(self.table, keys, values)
                try:
                    self.cursor.execute(sql)
                    self.con.commit()
                except Exception as e:
                    self.logger.info(e)
            else:
                self.logger.info('{}->{}->{} is exist!! pass it!'.format(tag1_name,tag2_name,book_name))
        except Exception as e:
            self.logger.info('sql:%s.detail:%s' % (select_sql, e))
            self.logger.info('{}->{}->{} select failed! pass it!'.format(tag1_name,tag2_name,book_name))

    def _create_table(self):
        try:
            self.cursor.execute('select * from {} limit 1'.format(self.table))
        except Exception as e:
            if e[-1] == u"Table '{}.{}' doesn't exist".format(self.db, self.table):
                sql_str_ = ','.join([i + ' varchar(100)' for i in KEYLIST if i != 'introduction'])
                self.cursor.execute('''create table {}(id int primary key auto_increment,{},introduction text,create_time_at timestamp default current_timestamp ) ENGINE=InnoDB DEFAULT CHARSET=utf8
                        '''.format(self.table, sql_str_)
                                    )


class DBBookSpider(object):
    index_url = 'https://book.douban.com/tag/'

    def __init__(self, start_page_num=1, end_page_num=3, logger=None):
        self.logger = logger or MyLogger().logger_instance
        assert isinstance(start_page_num, int) and isinstance(end_page_num,int) and end_page_num >= start_page_num, 'please offer the right pagenum range'
        self.start_page_num = start_page_num
        self.end_page_num = end_page_num
        self.headers = {
            'Host': 'book.douban.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'User-Agent': random.choice(USER_AGENT_LIST)
        }
        # self.proxies = None
        # self.proxies = {
        #     'https':'https://58.218.198.145:13145'
        # }
        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "H56HW8C40U338HPD"
        proxyPass = "EE29C7F7EDCFAC69"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }

    def get_response(self, url=None, data=None):
        url = url if url else self.index_url
        res = None
        for i in range(3):
            try:
                res = requests.get(url=url, params=data, headers=self.headers, proxies=self.proxies, timeout=10, )
                break
            except Exception as e:
                self.logger.info(e)
        return res

    def get_all_tags(self, response):
        if response:
            html = etree.HTML(response.text)
            tag1s = html.xpath('//div[@class="article"]/div[2]/div')
            assert tag1s, 'the tag1 parse wrong!! please check the html!!'
            for div in tag1s:
                item = {}
                tag1_name = div.xpath('./a/@name')[0]
                self.logger.info('start to parse {}'.format(tag1_name.encode('utf-8')))
                tag2s = div.xpath('.//td/a')
                for tag2 in tag2s:
                    tag2_name = tag2.xpath('./text()')[0]
                    self.logger.info('start to parse {}->{}'.format(tag1_name.encode('utf-8'),tag2_name.encode('utf-8')))
                    tag2_url_ = tag2.xpath('./@href')[0]
                    tag2_url = 'https://book.douban.com' + tag2_url_ if tag2_url_ else ''
                    item['tag1_name'] = re.sub('["\']','\\"',tag1_name)
                    item['tag2_name'] = re.sub('["\']','\\"',tag2_name)
                    item['tag2_url'] = tag2_url
                    if tag2_url:
                        for i in range(self.start_page_num, self.end_page_num + 1):
                            self.logger.info('start to get {} page {}'.format(tag2_name.encode('utf-8'), i))
                            data = {
                                'start': 20 * (i - 1),
                                'type': 'T'
                            }
                            res = self.get_response(tag2_url, data)
                            self._parse_tag(res, item)
                            self.logger.info('get {} page {} success!!'.format(tag2_name.encode('utf-8'), i))
                    else:
                        self.logger.info('get {} tag2_url failed!!'.format(tag2_name.encode('utf-8')))
                    self.logger.info('parse {}->{} success!!!'.format(tag1_name.encode('utf-8'),tag2_name.encode('utf-8')))
                self.logger.info('parse {} over!!'.format(tag1_name.encode('utf-8')))

    @staticmethod
    def get_md5(str1):
        md5_ = hashlib.md5()
        md5_.update(str1)
        return md5_.hexdigest()[8:-8]

    @staticmethod
    def _parse_publish_info(publish_info):
        publish_info_list = publish_info.strip().split('/')
        length = len(publish_info_list)
        if length == 5:
            result = [i.strip() for i in publish_info_list if i.strip()]
        elif length == 4:
            result = ['-'] + [i.strip() for i in publish_info_list if i.strip()]
        else:
            result = ['-'] * 5
        return result

    def _parse_tag(self, response, item):
        if response:
            html = etree.HTML(response.text)
            lis = html.xpath('//li[@class="subject-item"]')
            for li in lis:
                book_url = li.xpath('.//div[@class="info"]/h2/a/@href')[0] if li.xpath(
                    './/div[@class="info"]/h2/a/@href') else '-'
                book_name = li.xpath('.//div[@class="info"]/h2/a/text()')[0].strip() if li.xpath(
                    './/div[@class="info"]/h2/a/text()') else '-'
                re_list = re.findall('\d+', book_url)
                bid = re_list[0] if re_list else self.get_md5(book_name)

                publish_info_ = li.xpath('.//div[@class="pub"]/text()')[0]
                publish_info_list = self._parse_publish_info(publish_info_)
                publish_info_list = publish_info_list if len(publish_info_list) == 5 else ['-'] * 5
                item['foreign_author'] = re.sub('["\']','\\"',publish_info_list[0])
                item['chinese_author'] = re.sub('["\']','\\"',publish_info_list[1])
                item['publisher'] = re.sub('["\']','\\"',publish_info_list[2])
                item['publish_date'] = publish_info_list[3]
                item['price'] = publish_info_list[4]

                star_num_ = li.xpath('.//div[@class="star clearfix"]/span[contains(@class,"allstar")]/@class')
                star_num = star_num_[0].split('allstar')[-1] if star_num_ else '-'
                rating_nums_ = li.xpath('.//div[@class="star clearfix"]/span[@class="rating_nums"]/text()')
                rating_nums = rating_nums_[0] if rating_nums_ else '-'
                comment_num_ = li.xpath('.//div[@class="star clearfix"]/span[@class="pl"]/text()')
                comment_num = comment_num_[0].strip().strip('(').strip(')')
                introduction = li.xpath('.//div[@class="info"]/p/text()')[0] if li.xpath(
                    './/div[@class="info"]/p/text()') else '-'

                item['bid'] = bid
                item['book_url'] = book_url
                item['book_name'] = re.sub('["\']','\\"',book_name)
                item['star_num'] = star_num
                item['rating_nums'] = rating_nums
                item['comment_num'] = comment_num
                item['introduction'] = re.sub('["\']','\\"',introduction)
                Pipeline(logger=self.logger).to_mysql(item)


def main():
    spider = DBBookSpider()
    resp = spider.get_response()
    spider.get_all_tags(resp)


if __name__ == '__main__':
    main()
