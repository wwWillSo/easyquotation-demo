#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence 
@software: PyCharm
@file: easyquotation-multiprocessor.py
@time: 2017\10\17 0017 13:52
"""

from multiprocessing import Pool, Process
import os, time, random, easyquotation, pika, sys, traceback
import pprint
import redis

#定义新浪数据源

quotation = easyquotation.use("sina")

#获得rabbitMQ连接
connection=pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))

#将原列表按跨度分割成多个新列表
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def processor(name, codes) :
    channel = connection.channel()
    channel.exchange_declare(exchange='Clogs-'+name, exchange_type='fanout')
    channel.queue_declare(name)  # 如果有cc的队列,略过;如果没有,创建cc的队列

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
                if (name == 'mq-all' and k == '000001'):
                    print('进程%s：%s' % (name,v))
                channel.basic_publish(exchange='Clogs-'+name, routing_key='', body=v)
        except:
            traceback.print_exc()
        #单从展示来看理论上不需要查询得这么频繁
        time.sleep(1.3)

#创建进程池
def startPool() :
    # 获得配置文件中所有股票代码
    stock_codes = quotation.load_stock_codes()

    stock_codes_collections = list(chunks(stock_codes, 400))

    pool = Pool(len(stock_codes_collections))

    for i in range(0, len(stock_codes_collections)):
        result = pool.apply_async(processor, ('mq-'+str(i+1), stock_codes_collections[i]))

    # 多启动一个进程，发布全部代码行情
    p = Process(target=processor, args=('mq-all', stock_codes))
    p.start()

    pool.close()
    pool.join()

    connection.close()

    if result.successful():
        print('successful')

def syncToRedis():
    redis_client = redis.Redis(host='localhost', port=6379, db=1)
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
    startPool()