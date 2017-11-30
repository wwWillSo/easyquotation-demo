#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence 
@software: PyCharm
@file: easyquotation-zmq-publisher.py
@time: 2017\11\23 0023 11:24
"""

import zmq
import redis
import os, time, random, easyquotation, pika, sys, traceback
from multiprocessing import Pool, Process
import configparser

# 读取配置
config=configparser.ConfigParser()
config.read('config.ini')

#定义新浪数据源
quotation = easyquotation.use(config.get("easyquotation", 'source'))

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind(config.get("zmq", 'host'))

def processor(name, codes) :
    print('zmq-publisher开始运行...')
    while True:
        try:
            data = quotation.stocks(codes)
            for k, v in data.items():
                # print(k)
                k_dict = {'stockcode': k}
                v['date'] = v['date'] + ' ' + v['time']
                v['time'] = v['date']
                v = {**k_dict, **v}
                v = str(v)
                v = v.replace('\'', '\"')
                # if name == 'mq-all':
                # if (name == 'mq-all' and k == '000001'):
                #     print('进程%s：%s' % (name,v))
                socket.send_string('marketdata:' + k + '\r\n' + v)
        except:
            traceback.print_exc()
        #单从展示来看理论上不需要查询得这么频繁
        time.sleep(1.3)

# 获取redis_client
def getRedisClient() :
    host = config.get("redis", 'ip')
    port = config.get("redis", 'port')
    password = config.get("redis", 'password')
    db = config.get("redis", 'db')
    redis_client = redis.Redis(host=host, port=port, password=password, db=db)
    return redis_client

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

if __name__ == '__main__':
    syncToRedis()
    processor('mq-all', quotation.load_stock_codes())