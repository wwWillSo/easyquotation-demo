#!/usr/bin/env python
# encoding: utf-8

"""
@author: WillSo
@license: Apache Licence 
@software: PyCharm
@file: easyquotation-demo.py
@time: 2017\10\17 0017 9:11
"""

import easyquotation
import time
import pika
import sys
import traceback

quotation = easyquotation.use("sina")

connection=pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
channel=connection.channel()
channel.queue_declare(queue='cc')   #如果有cc的队列,略过;如果没有,创建cc的队列

start = time.clock()

while True :
    try:
        data = quotation.market_snapshot(prefix=True)
        for k, v in data.items() :
            print(k)
            k_dict = {'stockcode' : k}
            v = {**k_dict, **v}
            v = str(v)
            print(v)
            channel.basic_publish(exchange='', routing_key='cc', body=v)
    except:
        traceback.print_exc()

connection.close()

elapsed = (time.clock() - start)

print("Time used:",elapsed)

if __name__ == '__main__':
    pass