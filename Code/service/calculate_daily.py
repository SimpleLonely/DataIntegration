import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from service.factor_cal import *
from sqlalchemy import create_engine

engine = create_engine(
    'mysql+pymysql://root:767443924@119.3.210.244/data_integration')


def store_daily_factor(factors):
    try:
        factors.to_sql('factors', con=engine, index=True,
                       index_label='date', if_exists='append')
    except:
        pass

# call daily


def cal_daily_factor(date):
    '''
    每天调用一次，计算每天的因子数据，（默认该交易日有数据...
    :param date: 'yyyymmdd'
    :return:
    '''
    sql = 'select * from factors where date='+date
    df_factor_daily = pd.read_sql(sql, engine)

    # 已经有该日期的因子数据
    if not df_factor_daily.empty:
        print("repeated!")
        return

    else:
        sql = 'select * from daily_base where trade_date='+date
        df_basic = pd.read_sql_query(sql, engine)

        # 如果本日交易数据还没有
        if df_basic.empty:
            return

        sql = 'select * from daily_line where trade_date='+date
        df_daily = pd.read_sql_query(sql, engine)

        df = pd.merge(df_daily, df_basic, on='ts_code', how='inner')
        # print(df)
        smb, hml = cal_smb_hml(df)
        store_df = pd.DataFrame({'smb': smb, 'hml': hml}, index=[date])
        store_daily_factor(store_df)
        print(store_df)


def cal_past_factor():
    '''
    计算过去的所有因子并存入因子库
    :return:
    '''
    sql = 'select distinct trade_date from daily_line '
    df_trade_dates = pd.read_sql(sql, engine).sort_values(by='trade_date')
    for item in df_trade_dates['trade_date']:
        cal_daily_factor(item)
        print(item+" done")
        print()
