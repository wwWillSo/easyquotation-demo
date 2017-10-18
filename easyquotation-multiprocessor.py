#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence 
@software: PyCharm
@file: easyquotation-multiprocessor.py
@time: 2017\10\17 0017 13:52
"""

from multiprocessing import Pool
import os, time, random, easyquotation, pika, sys, traceback
import pprint

#定义新浪数据源
quotation = easyquotation.use("sina")

#获得rabbitMQ连接
connection=pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
channel=connection.channel()

#将原列表按跨度分割成多个新列表
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def processor(name, codes) :
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
                # if name == 'mq-11':
                print('进程%s：%s' % (name,v))
                channel.basic_publish(exchange='', routing_key=name, body=v)
        except:
            traceback.print_exc()
        #单从展示来看理论上不需要查询得这么频繁
        time.sleep(2)

#创建进程池
def startPool() :
    # 获得配置文件中所有股票代码
    stock_codes = quotation.load_stock_codes()

    stock_codes_collections = list(chunks(stock_codes, 400))

    pool = Pool(len(stock_codes_collections))

    for i in range(0, len(stock_codes_collections)):
        result = pool.apply_async(processor, ('mq-'+str(i+1), stock_codes_collections[i]))

    pool.close()
    pool.join()

    connection.close()

    if result.successful():
        print('successful')

if __name__ == '__main__':
    startPool()