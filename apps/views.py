# -*- coding: utf-8 -*-
from flask import Flask, url_for
from flask import render_template
import pymysql

app = Flask(__name__)


class GetMysqlData(object):
    def __init__(self, table='douban_books_info'):
        self.con = pymysql.connect(host='127.0.0.1', port=3306, user="root", password="", db="test", charset='utf8mb4')
        self.cursor = self.con.cursor()
        self.table = table

    def get_high_score_data(self,limit_num=100):
        sql="select book_name,chinese_author,publisher,rating_nums from {} order by rating_nums desc limit {}".format(self.table,limit_num)
        self.cursor.execute(sql)
        results=self.cursor.fetchall()
        return results


@app.route('/index/<int:num>')
def index(num):
    num=num or 100
    results=GetMysqlData().get_high_score_data(limit_num=num)
    return render_template('base.html',results=results)


@app.route('/login/<name>', methods=['GET', 'POST'])
def login(name):
    return 'login %s' % name


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5001)
