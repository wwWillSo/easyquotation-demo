#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence 
@software: PyCharm
@file: easyquotation-webservice.py
@time: 2017\11\13 0013 11:42
"""

import easyquotation
from flask import Flask, request as flaskReq, session, g, redirect, url_for, abort, \
     render_template, flash, jsonify
from urllib import request,parse
import re, os, traceback, time
import json
import redis
import configparser
import pymysql
from datetime import  datetime, timedelta

# 读取配置
config=configparser.ConfigParser()
config.read('config.ini')

quotation = easyquotation.use(config.get("easyquotation", 'source'))

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

#mysql配置
db_host = config.get("mysql", "host")
db_username = config.get("mysql", "username")
db_password = config.get("mysql", "password")
db_database = config.get("mysql", "database")

#将原列表按跨度分割成多个新列表
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

@app.route('/getMarketData')
def retrieve_marketdata():
    code = flaskReq.args.get('code')
    data = quotation.stocks(code)
    v = data.get(code)
    k_dict = {'stockcode': code}
    v['date'] = v['date'] + ' ' + v['time']
    v['time'] = v['date']
    v = {**k_dict, **v}
    v = str(v)
    v = v.replace('\'', '\"')
    print(v)
    return v

@app.route('/getAllMarketData')
def retrieve_all_marketdata():
    stock_codes = quotation.load_stock_codes()
    data = quotation.stocks(stock_codes)

    return_str = '{"marketdata" : ['

    for k, v in data.items():
        k_dict = {'stockcode': k}
        v['date'] = v['date'] + ' ' + v['time']
        v['time'] = v['date']
        v = {**k_dict, **v}
        v = str(v)
        v = v.replace('\'', '\"')
        return_str = return_str + v + ','
    return_str = return_str[:-1] + ']}'
    # print(return_str)
    return return_str

@app.route('/getAllDailyKLine')
def getAllDailyKLine() :
    stock_codes = quotation.load_stock_codes()

    all_k_line = {}

    all_k_line_arr = []

    i = 0
    for code in stock_codes :
        if i > 10 :
            break
        all_k_line_arr.append(getDailyKLineMethod(code))
        i = i + 1

    all_k_line['record'] = all_k_line_arr

    return jsonify(all_k_line)


def getDailyKLineMethod(stockcode):
    try:
        code = stockcode

        if int(code[0]) >= 6:
            code = 'sh' + code
        else:
            code = 'sz' + code

        url = 'http://api.finance.ifeng.com/akdaily/?code=' + code + '&type=last'
        req = request.Request(url)
        resp = request.urlopen(req)
        str = bytes.decode(resp.read(), encoding='utf-8')
        dict = json.loads(str)

        data_list = dict.get('record')

        cols_arr = ['date', 'open', 'high', 'close', 'low', 'volume', 'chg', '%chg', 'ma5', 'ma10', 'ma20',
                    'vma5', 'vma10', 'vma20', 'turnover']

        data_arr = []

        for data in data_list:
            i = 0
            # print(data)
            data_dict = {}
            for col in data:
                if cols_arr[i] == 'date' or cols_arr[i] == 'open' or cols_arr[i] == 'close' or cols_arr[
                    i] == 'high' or cols_arr[i] == 'low' or cols_arr[i] == 'volume' or cols_arr[
                    i] == 'turnover':
                    data_dict[cols_arr[i]] = col
                i = i + 1
                # print(data_dict)
            data_arr.append(data_dict)

        stock_dict = {}
        stock_dict[stockcode] = data_arr

        return stock_dict

    except:
        traceback.print_exc()


# http://api.finance.ifeng.com/akdaily/?code=sh601989&type=last
# ['date', 'open', 'high', 'close', 'low', 'volume','chg', '%chg', 'ma5', 'ma10', 'ma20','vma5', 'vma10', 'vma20', 'change']
# ['2014-12-01', '6.300', '6.450', '6.280', '6.150', '3689169.25', '-0.040', '-0.63', '6.280', '6.280', '6.280', '3,689,169.25', '3,689,169.25', '3,689,169.25', '2.26']
@app.route('/getDailyKLine')
def getDailyKLine() :
    try :
        stockcode = flaskReq.args.get('stockcode')
        code = stockcode

        if int(code[0]) >= 6 :
            code = 'sh' + code
        else :
            code = 'sz' + code

        url = 'http://api.finance.ifeng.com/akdaily/?code='+code+'&type=last'
        req = request.Request(url)
        resp = request.urlopen(req)
        str = bytes.decode(resp.read(), encoding='utf-8')
        dict = json.loads(str)

        data_list = dict.get('record')

        cols_arr = ['date', 'open', 'high', 'close', 'low', 'volume','chg', '%chg', 'ma5', 'ma10', 'ma20','vma5', 'vma10', 'vma20', 'change']

        data_arr = []

        history_switch = config.get("dailyKLineHistory", 'switch')

        stock_dict = {}

        if len(data_list) == 0:
            return jsonify(stock_dict)

        # 开启历史k线推送时遍历全部
        if history_switch == 'Y':
            for data in data_list:
                i = 0
                # print(data)
                data_dict = {}
                for col in data :
                    if cols_arr[i] == 'date' or cols_arr[i] == 'open' or cols_arr[i] == 'close' or cols_arr[i] == 'high' or cols_arr[i] == 'low' or cols_arr[i] == 'volume':
                        data_dict[cols_arr[i]] = col
                    i = i + 1
                # print(data_dict)
                data_arr.append(data_dict)
        else :
            data = data_list[len(data_list) - 1]
            i = 0
            # print(data)
            data_dict = {}
            for col in data:
                if cols_arr[i] == 'date' or cols_arr[i] == 'open' or cols_arr[i] == 'close' or cols_arr[i] == 'high' or \
                                cols_arr[i] == 'low' or cols_arr[i] == 'volume':
                    data_dict[cols_arr[i]] = col
                i = i + 1
            # print(data_dict)
            data_arr.append(data_dict)

        stock_dict[stockcode] = data_arr

        return jsonify(stock_dict)

    except:
        traceback.print_exc()

# 获取redis_client
def getRedisClient() :
    host = config.get("redis", 'ip')
    port = config.get("redis", 'port')
    password = config.get("redis", 'password')
    db = config.get("redis", 'db')
    redis_client = redis.Redis(host=host, port=port, password=password, db=db)
    return redis_client

@app.route('/syncToRedis')
def syncToRedis():
    redis_client = getRedisClient()
    stock_codes = list(quotation.load_stock_codes())
    #删除旧缓存
    redis_client.delete('stockCodes')

    data = quotation.stocks(stock_codes)
    # redis_client.set('stockCodes', list(data))
    str = ''
    for code in list(data):
        str = str + code + ','
    redis_client.set('stockCodes', "\"" + str + "\"")
    return "TRUE"

@app.route('/createNewTableJob')
def createNewTableJob():
    interval = flaskReq.args.get('interval')
    createNewTable(interval)
    return "SUCCESS"

def getConnect():
    db = pymysql.connect(db_host, db_username, db_password, db_database)
    return db
def createNewTable(interval) :
    db = None
    try :
        #market_data_candle_chart_2017_12_08
        print('转移行情表任务启动...')
        day = get_someday(int(interval))
        day = day.replace('-', '_')
        print(day)
        table_name = 'market_data_candle_chart_' + day
        sql1 = 'CREATE TABLE ' + table_name + ' like market_data_candle_chart;'
        sql2 = "insert into " + table_name + " (select * from market_data_candle_chart where create_time <= DATE_SUB(CURDATE(),INTERVAL " + str(
            interval) + " DAY) and chart_type not in (1440));"
        sql3 = "delete from market_data_candle_chart where create_time <= DATE_SUB(CURDATE(),INTERVAL " + str(
            interval) + " DAY)  and chart_type not in (1440);"
        print(sql1)
        print(sql2)
        print(sql3)
        db = getConnect()
        cursor = db.cursor()
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.execute(sql3)
        db.commit()
        print('转移行情表任务完成...')
    except:
        traceback.print_exc()
        return "FALSE"
    finally:
        db.close()

def copyData(interval) :
    db = None
    try:
        print('复制数据任务启动...')
        day = get_someday(interval)
        day = day.replace('-','_')
        print(day)
        table_name = 'market_data_candle_chart_' + day
        sql = "insert into " + table_name\
              + " (select * from market_data_candle_chart where DATE_SUB(CURDATE(),INTERVAL "+ str(interval) +" DAY) <= create_time and chart_type not in (1440) );"
        print(sql)
        db = getConnect()
        cursor = db.cursor()
        cursor.execute(sql)
        print('复制数据任务完成...')
    except :
        traceback.print_exc()
    finally :
        db.close()

def deleteData(interval) :
    db = None
    try:
        print('删除数据任务启动...')
        day = get_someday(interval)
        day = day.replace('-','_')
        print(day)
        table_name = 'market_data_candle_chart_' + day
        sql = "delete from market_data_candle_chart where DATE_SUB(CURDATE(),INTERVAL " + str(interval) + " DAY) <= create_time and chart_type not in (1440);"
        print(sql)
        db = getConnect()
        cursor = db.cursor()
        cursor.execute(sql)
        print('删除数据任务完成...')
    except :
        traceback.print_exc()
    finally :
        db.close()

def get_date(days=0):
    return datetime.now() - timedelta(days=days)

def get_someday(interval):
    day = datetime.strptime(str(get_date(interval)).split(' ')[0], '%Y-%m-%d')
    return str(day).split(' ')[0]

if __name__ == '__main__':
    # getDailyKLine()
    # getAllDailyKLine()
    app.run(debug=True, host=config.get("webservice", 'host'), port=int(config.get("webservice", 'port')))