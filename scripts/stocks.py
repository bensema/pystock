"""
stocks.py 获取上证和深证股票编码

"""
import os
import requests
import pymysql
import datetime
import warnings
from time import time
import pandas as pd

MYSQL_USER = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'stock'

PATH_DIR = '../data/stocks'
PATH_SH_MAIN_A = '../data/stocks/sh_main_a.xls'
PATH_SH_MAIN_B = '../data/stocks/sh_main_b.xls'
PATH_SH_STAR_MARK = '../data/stocks/sh_star_mark.xls'
PATH_SZ_MAIN_AND_CHI_NEXT = '../data/stocks/sz_main_a_and_chinext.xlsx'
PATH_SZ_MAIN_B = '../data/stocks/sz_main_b.xlsx'

conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    passwd=MYSQL_PASSWORD,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE)


class task:
    url = ''
    headers = {}
    save_path = ''

    def __init__(self, url, headers, save_path):
        self.url = url
        self.headers = headers
        self.save_path = save_path


CREATE_TABLE = '''
CREATE TABLE `stocks` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增编码',
  `code` varchar(64) NOT NULL DEFAULT '' COMMENT '股票代码',
  `short_name` varchar(255) NOT NULL DEFAULT '' COMMENT '简称',
  `listing_date` date NOT NULL DEFAULT '1970-01-01' COMMENT '上市日期',
  `exchange` enum('SH','SZ') NOT NULL DEFAULT 'SZ' COMMENT '交易所(SH:上海;SZ:深圳)',
  `market` enum('MAIN_A','MAIN_B','STAR_MARK','CHI_NEXT') NOT NULL DEFAULT 'MAIN_A' COMMENT '板块(MAIN_A:主板A;MAIN_B:主板B;STAR_MARK:科创板;CHI_NEXT:创业板)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`,`exchange`,`market`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;
'''

default_sz_headers = {
    "Referer": "http://www.szse.cn/market/product/stock/list/index.html",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36",
}

default_sh_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7,ru;q=0.6,ko;q=0.5,ja;q=0.4',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'query.sse.com.cn',
    'Pragma': 'no-cache',
    'Referer': 'https://www.sse.com.cn/',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',

}

task_list = [
    task(
        url='https://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE=1&COMPANY_STATUS=2,4,5,7,8',
        headers=default_sh_headers,
        save_path=PATH_SH_MAIN_A,
    ),
    task(
        url='http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE=2&COMPANY_STATUS=2,4,5,7,8',
        headers=default_sh_headers,
        save_path=PATH_SH_MAIN_B,
    ),
    task(
        url='http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE=8&COMPANY_STATUS=2,4,5,7,8',
        headers=default_sh_headers,
        save_path=PATH_SH_STAR_MARK,
    ),
    task(
        url='http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.18360749560659406',
        headers=default_sz_headers,
        save_path=PATH_SZ_MAIN_AND_CHI_NEXT,
    ),
    task(
        url='http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab2&random=0.7297641849234708',
        headers=default_sz_headers,
        save_path=PATH_SZ_MAIN_B,
    ),
]


def get(url, headers, stream=False):
    response = requests.get(url=url, headers=headers, stream=stream)
    return response


def download_source(url, headers, output_path):
    response = get(url, headers, stream=False)
    try:
        if not os.path.exists(PATH_DIR):
            os.makedirs(PATH_DIR)
    except OSError:
        pass
    with open(output_path, mode='wb') as f:
        f.write(response.content)


# 上海交易所 主板A
def parse_sh_main_a():
    df = pd.read_excel(PATH_SH_MAIN_A, usecols=["公司代码", "公司简称", "上市日期"])
    # data = df.head()  # 默认读取前5行的数据
    # print(data)  # 格式化输出
    cur = conn.cursor()
    sql = "insert ignore into stocks (code,short_name,listing_date,exchange,market) values(%s,%s,%s,%s,%s)"
    try:
        insert_list = []
        for d in df.values:
            insert_list.append((d[0], d[1], datetime.datetime.strptime(str(d[2]), "%Y%m%d"), 'SH', 'MAIN_A'))
        insert = cur.executemany(sql, insert_list)
        print('批量插入返回受影响的行数：', insert)
        conn.commit()
    except Exception as e:
        print('error :%s', str(e))
    finally:
        cur.close()


# 上海交易所 主板B
def parse_sh_main_b():
    df = pd.read_excel(PATH_SH_MAIN_B, usecols=["B股代码", "公司简称", "上市日期"])
    # data = df.head()  # 默认读取前5行的数据
    # print(data)  # 格式化输出
    cur = conn.cursor()
    sql = "insert ignore into stocks (code,short_name,listing_date,exchange,market) values(%s,%s,%s,%s,%s)"
    try:
        insert_list = []
        for d in df.values:
            insert_list.append((d[0], d[1], datetime.datetime.strptime(str(d[2]), "%Y%m%d"), 'SH', 'MAIN_B'))
        insert = cur.executemany(sql, insert_list)
        print('批量插入返回受影响的行数：', insert)
        conn.commit()
    except Exception as e:
        print('error :%s', str(e))
    finally:
        cur.close()


# 上海交易所 科创板
def parse_sh_star_mark():
    df = pd.read_excel(PATH_SH_STAR_MARK, usecols=["公司代码", "公司简称", "上市日期"])
    cur = conn.cursor()
    sql = "insert ignore into stocks (code,short_name,listing_date,exchange,market) values(%s,%s,%s,%s,%s)"
    try:
        insert_list = []
        for d in df.values:
            insert_list.append((d[0], d[1], datetime.datetime.strptime(str(d[2]), "%Y%m%d"), 'SH', 'MAIN_B'))
        insert = cur.executemany(sql, insert_list)
        print('批量插入返回受影响的行数：', insert)
        conn.commit()
    except Exception as e:
        print('error :%s', str(e))
    finally:
        cur.close()


# 深圳交易所 主板A+创业板
def parse_sz_main_a_and_chi_next():
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        df = pd.read_excel(PATH_SZ_MAIN_AND_CHI_NEXT, usecols=["A股代码", "A股简称", "A股上市日期", "板块"])

    cur = conn.cursor()
    sql = "insert ignore into stocks (code,short_name,listing_date,exchange,market) values(%s,%s,%s,%s,%s)"
    try:
        insert_list = []
        for d in df.values:
            if (d[0] == "主板"):
                market = "MAIN_A"
            else:
                market = "CHI_NEXT"
            insert_list.append((d[1], d[2], datetime.datetime.strptime(str(d[3]), "%Y-%m-%d"), 'SZ', market))
        insert = cur.executemany(sql, insert_list)
        print('批量插入返回受影响的行数：', insert)
        conn.commit()
    except Exception as e:
        print('error :%s', str(e))
    finally:
        cur.close()


# 深圳交易所 主板B
def parse_sz_main_b():
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        df = pd.read_excel(PATH_SZ_MAIN_B, usecols=["板块", "B股代码", "B股简称", "B股上市日期", ])

    cur = conn.cursor()
    sql = "insert ignore into stocks (code,short_name,listing_date,exchange,market) values(%s,%s,%s,%s,%s)"
    try:
        insert_list = []
        for d in df.values:
            market = "MAIN_B"
            insert_list.append((d[1], d[2], datetime.datetime.strptime(str(d[3]), "%Y-%m-%d"), 'SZ', market))
        insert = cur.executemany(sql, insert_list)
        print('批量插入返回受影响的行数：', insert)
        conn.commit()
    except Exception as e:
        print('error :%s', str(e))
    finally:
        cur.close()


def main():
    for task in task_list:
        try:
            print("start download ", task.save_path)
            start = time()
            download_source(task.url, task.headers, task.save_path)
            elapsed = (time() - start)
            print("end download. time used:", elapsed)
        except Exception as e:
            print('download error :', str(e))

    cur = conn.cursor()
    cur.execute(CREATE_TABLE)
    try:
        parse_sh_main_a()
        parse_sh_main_b()
        parse_sh_star_mark()
        parse_sz_main_a_and_chi_next()
        parse_sz_main_b()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
