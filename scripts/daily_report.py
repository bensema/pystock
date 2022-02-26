"""
daily_report.py 获取上证和深证股票 日k

"""
import pymysql
import time
import requests
import json
import datetime

from settings import *

CREATE_TABLE = '''
CREATE TABLE `daily_report` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增编码',
  `code` varchar(6) NOT NULL DEFAULT '' COMMENT '股票代码',
  `trade_date` varchar(8) NOT NULL DEFAULT '' COMMENT '交易日期',
  `open` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '开盘价',
  `high` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '最高价',
  `low` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '最低价',
  `close` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '收盘价',
  `pre_close` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '昨收价',
  `change` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '涨跌额',
  `pct_chg` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '涨跌幅(%)',
  `volume` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '成交量(股)',
  `amount` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '成交额(元)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code_2` (`code`,`trade_date`) USING BTREE,
  KEY `code` (`code`) USING BTREE,
  KEY `trade_date` (`trade_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;
'''

conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    passwd=MYSQL_PASSWORD,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE)

default_headers = {
    "Accept": "*/*",
    "symbol": "getline",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7,ru;q=0.6,ko;q=0.5,ja;q=0.4",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Cookie": "yfx_c_g_u_id_10000042=_ck22022301174611653851305055551; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_mr_f_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; VISITED_INDEX_CODE=%5B%22000001%22%5D; seecookie=%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C%2C%5B600008%5D%3A%u9996%u521B%u73AF%u4FDD; VISITED_COMPANY_CODE=%5B%22600008%22%2C%22600000%22%5D; VISITED_STOCK_CODE=%5B%22600008%22%2C%22600000%22%5D; VISITED_MENU=%5B%2212692%22%2C%228314%22%2C%228317%22%2C%228453%22%2C%228466%22%2C%228528%22%2C%229062%22%2C%2211323%22%2C%228774%22%2C%229055%22%2C%229060%22%5D; yfx_f_l_v_t_10000042=f_t_1645550266120__r_t_1645754945696__v_t_1645779804205__r_c_2",
    "Host": "yunhq.sse.com.cn:32041",
    "Pragma": "no-cache",
    "Referer": "http://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36",
}


def get(url, headers, stream=False):
    response = requests.get(url=url, headers=headers, stream=stream)
    return response


# 上海交易所 A股数据，至2022/02/25 大约570万条记录
def sh_daily_history(begin=100000):
    cur = conn.cursor()
    sql = "select code from stocks where exchange=%s'"
    try:
        cur.execute(sql, EXCHANGE_SH)

        results = cur.fetchall()

        url_format = 'http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{}?begin={}&end={}&period=1&select=date,open,high,low,close,volume,amount,prevClose&recovered=&today=y&_={}'

        for row in results:
            code = row[0]
            t = time.time()
            _t = int(round(t * 1000))
            today = datetime.date.today()
            formatted_today = today.strftime('%Y%m%d')
            url = url_format.format(code, begin, formatted_today, _t)
            response = get(url, default_headers)

            json_data = json.loads(response.content)
            insert_list = []
            insert_sql = "insert ignore into daily_report (code,trade_date,open,high,low,close,volume,amount,pre_close) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for kline in json_data["kline"]:
                insert_list.append((
                    code, kline[0], kline[1], kline[2], kline[3], kline[4], kline[5], kline[6], kline[7]
                ))
            insert = cur.executemany(insert_sql, insert_list)
            conn.commit()
            print(code, 'total:', json_data['total'], ' 批量插入返回受影响的行数：', insert)
    except Exception as e:
        print(e)
    finally:
        cur.close()


def create_table():
    cur = conn.cursor()
    try:
        cur.execute(CREATE_TABLE)
    except:
        pass
    finally:
        cur.close()


def main():
    sh_daily_history()


if __name__ == '__main__':
    try:
        create_table()
        main()
    finally:
        conn.close()
