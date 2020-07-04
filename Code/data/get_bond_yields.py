import requests
import pandas as pd
from sqlalchemy import create_engine
import time
engine = create_engine(
    'mysql+pymysql://root:767443924@119.3.210.244/data_integration')

HEADERS = {
    "Host": "www.chinamoney.com.cn",
    "Connection": "keep-alive",
    "Content-Length": "0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "http://www.chinamoney.com.cn",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",
    "Referer": "http://www.chinamoney.com.cn/chinese/sddsintigy/",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cookie": "_ulta_id.CM-Prod.e9dc=f8cf4ffa5a16402d; A9qF0lbkGa=MDAwM2IyNTRjZDAwMDAwMDAwNGYwHXtLe0gxNTkzNDQyOTY1; _ulta_ses.CM-Prod.e9dc=6df4adb156c0c873"
}


def get_bond_yields(startDate, endDate):
    params = {
        'startDate': startDate,
        'endDate': endDate,
        'pageNum': 1,
        'pageSize': 400
    }

    url = "http://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/SddsIntrRateGovYldHis"
    res = requests.get(url=url, params=params, headers=HEADERS)
    print(res)
    data = res.json()
    return data


def get_bond_yields_history():
    for i in range(2017, 2021):
        startDate = str(i)+'-01-01'
        endDate = str(i)+'-12-31'
        data = get_bond_yields(startDate, endDate)
        data = data['records']
        df = pd.DataFrame(data)
        df = df.loc[:, ['dateString', 'oneRate', 'tenRate']]
        df['dateString'] = df['dateString'].apply(lambda x: x.replace('-', ''))
        df = df.set_index('dateString')
        df.to_sql('bond_yields', con=engine, index=True,
                  index_label='date', if_exists='append')


def get_daily_yields_history(date):

    sql = 'select * from bond_yields where date='+date
    df_bond_yields_daily = pd.read_sql(sql, engine)

    # 已经有该日期的收益率数据
    if not df_bond_yields_daily.empty:
        print("repeated!")
        return
    date=date[:4]+"-"+date[4:6]+"-"+date[6:]
    data = get_bond_yields(date, date)
    if len(data) == 0:
        pass
    else:
        data = data['records']
        df = pd.DataFrame(data)
        print(df)
        df = df.loc[:, ['dateString', 'oneRate', 'tenRate']]
        df['dateString'] = df['dateString'].apply(lambda x: x.replace('-', ''))
        df = df.set_index('dateString')
        print(df)
        df.to_sql('bond_yields', con=engine, index=True,
                  index_label='date', if_exists='append')

# get_daily_yields_history('20200630')
