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
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("china_trade.log", encoding='utf-8'), # 儲存到檔案
        logging.StreamHandler() # 同時噴喺 Console
    ]
)

# %%
now=pd.Timestamp.now().replace(day=1).strftime('%Y-%m-%d')

logging.info(f"🚀 程式啟動。目前設定日期: {now}")
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

def get_china_trade_by_country(date: int) -> pd.DataFrame:
    logging.info(f"正在爬取日期: {date} ...")
    s = requests.Session()
    s.mount('https://', TLSAdapter())
    url=r'https://data.mofcom.gov.cn/datamofcom/front/totalbycountry/query'
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    }

    playload={'date': date}

    # 之後如常使用 s 進行請求
    try:
        response = s.post(url, headers=headers, data=playload, timeout=30) # 加入 timeout 避免死等
        response.raise_for_status() # 如果 Status Code 唔係 200 會直接跳去 except
        time.sleep(5)

        json_data = response.json()
        if 'rows' in json_data:
            df = pd.DataFrame(json_data['rows'])
            logging.info(f"✅ 成功獲取 {date} 數據，共 {len(df)} 條紀錄。")
            return df
        else:
            logging.warning(f"⚠️ {date} 回傳 JSON 格式異常，搵唔到 'rows'。")
            return pd.DataFrame()
        # json_data = response.json()

        # if response.status_code==200:
        #     logging.info(f"連線成功。爬取數據日期: {date}")
        #     data=response.json()['rows']
        #     df=pd.DataFrame(data)
        #     return df
        # else:
        #     logging.error(f"連線失敗。爬取數據日期: {date}")
        #     return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ {date} 連線失敗: {e}")
    except Exception as e:
        logging.error(f"❌ {date} 發生意外錯誤: {e}")
    
    return pd.DataFrame()

# %%
dfs=[get_china_trade_by_country(i) for i in date_range_str]
dfs = [d for d in dfs if not d.empty]
# %%
if dfs:
    all_dates_txt = ", ".join(map(str, date_range_str))
    logging.info(f"📦 正在合併並更新數據庫，日期範圍: {all_dates_txt}")

    df=pd.concat(dfs)
    df['updated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # %%
    db_file_name=r'china_mocom.db'
    db_table_name = r'by_country'
    unique_keys = ['trade_date', 'type']

    try:
        engine = create_engine(f'sqlite:///{db_file_name}')
        create_update_db(engine, db_table_name, df, unique_keys)
        logging.info("🎉 數據庫更新成功！")
    except Exception as e:
        logging.error(f"💀 更新數據庫時崩潰: {e}")

else:
    all_dates_txt = ", ".join(map(str, date_range_str))
    logging.warning(f"沒有數據。爬取數據日期: {all_dates_txt}")