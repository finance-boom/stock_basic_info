#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import time
import json
import math
import datetime
import sys
import urllib
import importlib

import pandas
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
importlib.reload(sys)
sys.path.append('/home/shili.lzy/boom/chisong.xjm/lib')
from MysqlLib import MysqlClass
import matplotlib.pyplot as plt 

class StockBasicInfo:
    """获取股票基础信息类，包含换手率，流通股数，市盈率，收盘价，成交额
    """

    def __init__(self):
        self.nmysql=MysqlClass('root5','Xjm@123456')
        self.market_value_back_year = 5
        self.market_value_blank_back_day = 365 * self.market_value_back_year
        self.start_date=self.nmysql.getDate(- \
                self.market_value_blank_back_day - 1)
        self.end_date=self.nmysql.getDate(0)

    def get_SHSZcode_order_by_market_value(self, date):
        """返回深圳和上海所有股票的代码，按照市值降序
        input: NULL
        output: 如果失败返回-1，如果成功返回tuple股票代码序列
        """
        query = "select sms.code,sms.update_time, \
                sms.circular_market_val from stock_market_snapshot as \
                sms join stock_uid_info as sui on \
                sms.code=sui.code where sui.market_code='SH.3000005' and \
                sms.update_time=" + date + 
                "sms.circular_market_val != 0 order by \
                sms.circular_market_val DESC"
        tmv=self.nmysql.Query(query)
        if len(tmv) <= 0:
            return -1
        else:
            return tmv

    def get_partition_by_market_value(self, partitionN):
        """返回分割成N份的上海和深圳的股票代码，按照市值降序
        input: 分隔的份数
        output: 如果失败返回-1，如果成功返回map，key是topN，value是股票代码tuple
        """
        if partitionN <= 0:
            return -1
        tmv = self.get_SHSZcode_order_by_market_value()
        if tmv == -1:
            return -1
        stock_total_num = len(tmv)
        if stock_total_num <= 0:
            return -1
        partition_step = math.floor(stock_total_num / partitionN)
        if partition_step <= 1:
            return -1
        code_dict = {}
        for one in range(1, partitionN+1):
            key = "top" + str(one)
            start_point = partition_step * (one - 1)
            end_point = partition_step * one
            code_dict[key] = tmv[start_point:end_point]
        return code_dict

    def get_close_price_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的收盘价
        input: 股票代码，日期
        output: 如果失败返回-1，如果成功返回该股票收盘价
        """
        run_sql="select nclose from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            return result_sql[0][0]
        else:
            return -1

    def get_PE_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的市盈率
        input: 股票代码，日期$
        output: 如果失败返回-1，如果成功返回该股票市盈率
        """
        run_sql="select pe_ratio from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            return result_sql[0][0]
        else:
            return -1
    def get_PE_dict_by_stock_tuple(self, code_tuple, date):
        """返回指定code元组，指定日期的pe值
        input: 股票代码元组序列，日期$
        output: 如果失败返回-1，如果成功返回该股票序列对应的pe值，
                dict，key为code，value为pe
        """
        pe_dict = {}
        if len(code_tuple) <= 0:
            return -1
        for one in code_tuple:
            if len(one) <= 0:
                break
            else:
                pe_dict[one[0]] = self.get_PE_by_stock_code(one[0], date)
        return pe_dict
    
    def get_turnover_GMV_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的成交额
        input: 股票代码，日期$
        output: 如果失败返回-1，如果成功返回该股票成交额
        """
        run_sql="select turnover from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            return result_sql[0][0]
        else:
            return -1 

    def get_turnover_ratio_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的换手率
        input: 股票代码，日期$
        output: 如果失败返回-1，如果成功返回该股票换手率
        """
        run_sql="select turnover_rate from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            return result_sql[0][0]
        else:
            return -1 

    def get_volume_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的成交量
        input: 股票代码，日期$
        output: 如果失败返回-1，如果成功返回该股票成交量
        """
        run_sql="select volume from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            return result_sql[0][0]
        else:
            return -1

    def get_outstanding_shares_by_stock_code(self, code, date):
        """返回指定code代码+指定日期的流通总股数
        input: 股票代码，日期$
        output: 如果失败返回-1，如果成功返回该股票流通总股数
        """
        run_sql="select volume,turnover_rate from stock_day_kline where \
                time_key=\'" + date + "\' and code=\'" + code + "\'"
        result_sql=sorted(self.nmysql.Query(run_sql))
        if len(result_sql) > 0:
            volume = result_sql[0][0]
            turnover_ratio = result_sql[0][1]
            if turnover_ratio <= 0:
                return -1
            else:
                return int(volume * 100 / turnover_ratio)
        else:
            return -1
    
    def get_normalized_profit_by_stock_code(self, code, start, end):
        """返回指定code代码+指定日期区间的归一化收益，排除增发影响，排除休市影响
        input: 股票代码，开始日期，结束日期$
        output: 如果失败返回-1，如果成功返回该股票归一化收益
        """
        run_sql = "select volume,turnover_rate,nclose,time_key from \
                  stock_day_kline where \
                  time_key between \'" + start + "\' and \'" + end + "\' and code=\'" \
                  + code + "\'" + " order by time_key DESC"
        result_sql = self.nmysql.Query(run_sql)
        start_issued_shares = 0
        start_price = 0
        end_issued_shares = 0
        end_price = 0
        if len(result_sql) > 1:
            start_volume = result_sql[-1][0]
            start_turnover_ratio = result_sql[-1][1]
            start_price = result_sql[-1][2]
            end_volume = result_sql[0][0]
            end_turnover_ratio = result_sql[0][1]
            end_price = result_sql[0][2]
            if start_turnover_ratio <= 0:
                return -1
            else:
                if start_turnover_ratio <= 0 and end_turnover_ratio > 0:
                    end_issued_shares = int(end_volume / end_turnover_ratio)
                    start_issued_shares = end_issued_shares
                elif start_turnover_ratio > 0 and end_turnover_ratio <= 0:
                    start_issued_shares = int(start_volume / start_turnover_ratio)
                    end_issued_shares = start_issued_shares
                elif start_turnover_ratio <= 0 and end_turnover_ratio <= 0:
                    return -1
                else:
                    start_issued_shares = int(start_volume / start_turnover_ratio)
                    end_issued_shares = int(end_volume / end_turnover_ratio)
        else:
            return -1
        if start_issued_shares <= 0 or start_price <= 0:
            return -1
        else:
            #print("start_issued_shares:%d start_price:%s end_issued_shares:%d \
            #     end_price:%s" %(start_issued_shares, start_price, \
            #     end_issued_shares, end_price))
            #print((end_issued_shares / start_issued_shares) *\
            #     (end_price - start_price) / start_price)
            return (end_issued_shares / start_issued_shares) * \
                   (end_price - start_price) / start_price


    def cal_stock_profit_ratio(self, stock_tuple, start, end):
        """指定开始和结束日期，股票序列的收益率
        input: 股票代码序列，开始日期，结束日期$
        output: 如果失败返回-1，如果成功返回该序列收益率，每只股票的都按照1元买入
        """
        total = 0
        wight = 1
        for one in stock_tuple:
            profit_ratio_one = self.get_normalized_profit_by_stock_code( \
                               one[0], start, end)
            if profit_ratio_one == -1:
                continue
            total += profit_ratio_one * wight
            #print("code=%s,start=%s end=%s\ profit=%s total=%s" %(one[0], \
            #      start, end, profit_ratio_one, total))
        profit_total_ratio = total / len(stock_tuple)
        #print("profit_total_ratio=%s" %(profit_total_ratio))
        return profit_total_ratio

if __name__ == '__main__':
    partitionN = 10
    stock_test = StockBasicInfo()
    #print(stock_test.get_normalized_profit_by_stock_code("SH.601398", '2018-12-17', '2019-01-01'))
    #print(stock_test.get_normalized_profit_by_stock_code("SH.601398", '2018-12-29', '2019-01-01'))
    shsz_stock_dict = {}
    #shsz_stock_dict['top1'] = (('SZ.000002', '2018-12-12', 1.0), ('SH.601857', '2018-12-19', 1.0))
    #shsz_stock_dict['top2'] = (('SZ.000001', '2018-12-12', 1.0), ('SH.601398', '2018-12-19', 1.0))
    #print(stock_test.cal_stock_profit_ratio(shsz_stock_dict['top1'], stock_test.start_date, stock_test.end_date))
    #print(stock_test.cal_stock_profit_ratio(shsz_stock_dict['top2'], stock_test.start_date, stock_test.end_date))
    shsz_stock_dict = stock_test.get_partition_by_market_value(partitionN)
    for one in range(1, partitionN+1):
        index = 'top' + str(one)
        profit_total_ratio = stock_test.cal_stock_profit_ratio( \
                shsz_stock_dict[index], \
                stock_test.start_date, stock_test.end_date)
        print("%s profitratio in %s and %s: %f" %(index, stock_test.start_date, \
                    stock_test.end_date, profit_total_ratio))
