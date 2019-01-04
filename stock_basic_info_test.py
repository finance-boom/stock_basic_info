#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import time
from stock_basic_info import StockBasicInfo

class StockBasicInfoTestCase(unittest.TestCase):
    """StockBasicInfo类的单元测试用例
    """
    def setUp(self):
        self.stock_basic_info = StockBasicInfo()
        self.MAX_CHECK_DAY = 10
        self.SECONDS_ONE_DAY = 86400
        self.stock_code_tuple = \
            self.stock_basic_info.get_SHSZcode_order_by_market_value()

    def tearDown(self):
        self.stock_basic_info = None

    def test_get_SHSZcode_order_by_market_value_len(self):
        """股票市值的测试用例
        测试股票市值的tuple返回的长度是否在[1000, 100000]之间
        """
        stock_code_tuple = self.stock_code_tuple
        checker_len = True
        #测试返回对象的长度
        if stock_code_tuple == -1:
            checker_len = False
        else:
            tuple_len = len(stock_code_tuple)
            if tuple_len > 100000:
                checker_len = False
            elif tuple_len < 1000:
                checker_len = False
            else:
                checker_len = True
        self.assertTrue(checker_len)

    def test_get_SHSZcode_order_by_market_value_exist(self):
        """股票市值的测试用例
        测试A股股票市值中是否有上证的中国工商银行SH.601398
        测试A股股票市值中是否有深证的平安银行SZ.000001
        """
        #测试是否存在SH.601398和SZ.000001
        stock_code_tuple = self.stock_code_tuple
        checker_exist = True
        counter = 0
        for one in stock_code_tuple:
            if 'SH.601398' in one:
                counter += 1
            if 'SZ.000001' in one:
                counter += 1
        if counter < 2:
            checker_exist = False
        self.assertTrue(checker_exist)

    def test_get_SHSZcode_order_by_market_value_update(self):
        """股票市值的测试用例
        测试SH.601398和SZ.000001是否最后更新时间都在15日之内
        """
        #测试SH.601398和SZ.000001是否最后更新时间都在15日之内
        stock_code_tuple = self.stock_code_tuple
        checker_update = True
        counter = 0
        for one in stock_code_tuple:
            if 'SH.601398' in one:
                update_time = one[1]
                get_time = time.mktime(time.strptime(update_time,"%Y-%m-%d %H:%M:%S"))
                now_time = time.time()
                if now_time - get_time > self.SECONDS_ONE_DAY * self.MAX_CHECK_DAY:
                    continue
                else:
                    counter += 1
            if 'SZ.000001' in one:
                update_time = one[1]
                get_time = time.mktime(time.strptime(update_time,"%Y-%m-%d %H:%M:%S"))
                now_time = time.time()
                if now_time - get_time > self.SECONDS_ONE_DAY * self.MAX_CHECK_DAY:
                    continue
                else:
                    counter += 1
        if counter < 2:
            checker_update = False
        self.assertTrue(checker_update)

    def test_get_partition_by_market_value(self):
        """按市值排序分隔股票测试
        测试当分隔份数为0时的返回，是否等于-1
        测试当分隔份数为-1时的返回，是否等于-1
        测试当分隔份数为10时的返回，是否等于len(self.stock_code_tuple)/10 or +1
        """
        checker_partition = True
        error_msg = "NULL"
        stock_partition = self.stock_basic_info.get_partition_by_market_value(\
                0)
        if stock_partition == -1:
            pass
        else:
            checker_partition = False
            error_msg = "partitionN=0, get no -1"
        stock_partition = self.stock_basic_info.get_partition_by_market_value(\
                -1)
        if stock_partition == -1:
            pass
        else:
            checker_partition = False
            error_msg = "partitionN=-1, get no -1"
        stock_partition = self.stock_basic_info.get_partition_by_market_value(\
                10)
        if stock_partition == -1:
            checker_partition = False
            error_msg = "partitionN=10, get -1"
        else:
            if len(stock_partition['top1']) < len(self.stock_code_tuple)/10 - 1\
               or \
               len(stock_partition['top1']) > len(self.stock_code_tuple)/10 + 1:  
                checker_partition = False
                error_msg = "partitionN=10, get length " + \
                str(len(stock_partition['top1'])) + "total len:" + \
                str(len(self.stock_code_tuple))
            else:
                pass
        self.assertTrue(checker_partition, error_msg)

    def test_cal_stock_profit_ratio(self):
        """测试计算股票集收益
        测试输入万科SZ.000002+SH.601857，时间段为2013-12-20~2018-12-20结果
        """
        shsz_stock_dict = {}
        shsz_stock_dict['top1'] = (('SZ.000002', '2018-12-12', 1.0), \
                                   ('SH.601857', '2018-12-19', 1.0))
        start_date = '2013-12-20'
        end_date = '2018-12-20'
        checker_profit = True
        expect_profit = 2.34
        error_msg = "ok"
        profit = self.stock_basic_info.cal_stock_profit_ratio(\
                 shsz_stock_dict['top1'], start_date, end_date)
        profit = round(profit, 2)
        if profit != expect_profit:
            checker_profit = False
            error_msg = "get SZ.000002+SH.601857，时间段为2013-12-20~2018-12-20 \
                        not " + str(expect_profit)
        self.assertTrue(checker_profit, error_msg)

    def test_get_normalized_profit_by_stock_code(self):
        """测试计算股票集收益
        测试输入工商银行SH.601398，指定时间段的输出结果
        """
        checker_profit = True
        error_msg = "ok"
        expect_profit = -0.015
        profit_ratio1 = self.stock_basic_info.get_normalized_profit_by_stock_code(\
                "SH.601398", '2018-12-17', '2019-01-01')
        profit_ratio1 = round(profit_ratio1, 3)
        if profit_ratio1 != expect_profit:
            checker_profit = False
            error_msg = "SH.601398, '2018-12-17', \
                         '2019-01-01',profit_ratio is not " + str(expect_profit)
        profit_ratio2 = self.stock_basic_info.get_normalized_profit_by_stock_code(\
                "SH.601398", '2018-12-29', '2019-01-01')
        profit_ratio2 = round(profit_ratio2, 3)
        if profit_ratio2 != -1:
            checker_profit = False
            error_msg = "SH.601398, '2018-12-29', \
                         '2019-01-01',profit_ratio is not -1"
        self.assertTrue(checker_profit, error_msg)

if __name__ == "__main__":
    # 构造测试集
    suite = unittest.TestSuite()
    suite.addTest(StockBasicInfoTestCase( \
                "test_get_SHSZcode_order_by_market_value_len"))
    suite.addTest(StockBasicInfoTestCase( \
                "test_get_SHSZcode_order_by_market_value_exist"))
    suite.addTest(StockBasicInfoTestCase( \
                "test_get_SHSZcode_order_by_market_value_update"))
    suite.addTest(StockBasicInfoTestCase( \
                "test_get_partition_by_market_value"))
    suite.addTest(StockBasicInfoTestCase( \
                "test_cal_stock_profit_ratio"))
    suite.addTest(StockBasicInfoTestCase( \
                "test_get_normalized_profit_by_stock_code"))
    
    # 执行测试
    runner = unittest.TextTestRunner()
    runner.run(suite)
