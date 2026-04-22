# %%
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import pandas as pd
import time
import sqlite3
from update_db_func import *
from sqlalchemy import create_engine
import logging

# 基本設定
logging.basicConfig(
    level=logging.INFO, # 設定顯示邊個等級以上嘅訊息
    format='%(asctime)s - %(levelname)s - %(message)s', # 設定格式：時間 - 等級 - 內容
    datefmt='%Y-%m-%d %H:%M:%S'
)

# %%
now=pd.Timestamp.now().replace(day=1).strftime('%Y-%m-%d')

logging.info(f"日期: {now}, 程式開始執行喇...")
# %%
date_range = pd.date_range(end=now, periods=3, freq='ME')
date_range_str=[int(i.strftime('%Y%m')) for i in date_range]

# %%
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        # 手動創建 SSL Context 並指定支援的加密套件
        context = create_urllib3_context()
        # 允許較舊的加密算法 (下放安全性)
        context.set_ciphers('DEFAULT@SECLEVEL=1') 
        kwargs['ssl_context'] = context
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def get_china_trade_by_country(date):
    s = requests.Session()
    s.mount('https://', TLSAdapter())
    url=r'https://data.mofcom.gov.cn/datamofcom/front/totalbycountry/query'
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    }

    playload={'date': date}

    # 之後如常使用 s 進行請求
    response = s.post(url, headers=headers, data=playload)
    time.sleep(5)
    if response.status_code==200:
        logging.info(f"連線成功。爬取數據日期: {date}")
        data=response.json()['rows']
        df=pd.DataFrame(data)
        return df
    else:
        logging.error(f"連線失敗。爬取數據日期: {date}")
        return pd.DataFrame()

# %%
dfs=[get_china_trade_by_country(i) for i in date_range_str]

# %%
if dfs:
    logging.info(f"有數據。爬取數據日期: {", ".join(date_range_str)}")
    df=pd.concat(dfs)
    df['updated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # %%
    db_file_name=r'china_mocom.db'
    db_table_name = r'by_country'
    unique_keys = ['trade_date', 'type']
    engine = create_engine(f'sqlite:///{db_file_name}')
    
    # create_db(engine, db_table_name, df, unique_keys)
    create_update_db(engine, db_table_name, df, unique_keys)

else:
    logging.warning(f"沒有數據。爬取數據日期: {", ".join(date_range_str)}")