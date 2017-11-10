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
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, jsonify

quotation = easyquotation.use("sina")

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

#将原列表按跨度分割成多个新列表
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

@app.route('/getMarketData')
def retrieve_marketdata():
    code = request.args.get('code')
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
    print(return_str)
    return return_str

if __name__ == '__main__':
    app.run(debug=True, port=8081)