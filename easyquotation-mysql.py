#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence
@software: PyCharm
@file: easyquotation-zmq-publisher.py
@time: 2017\11\23 0023 11:24
"""

import configparser
import pymysql

# 打开数据库连接
db = pymysql.connect("localhost", "testuser", "test123", "TESTDB")

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()

print("Database version : %s " % data)

# 关闭数据库连接
db.close()

# 读取配置
config=configparser.ConfigParser()
config.read('config.ini')

if __name__ == '__main__':
    pass