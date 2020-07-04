import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime
import statsmodels.api as sm
import tushare as ts
from sqlalchemy import create_engine
import numpy as np
import time
from data.return_rate_dao import *

engine=create_engine('mysql+pymysql://root:767443924@119.3.210.244/data_integration')

ts.set_token('ed713a2e27e3930b752d0638476753e267a66c1a6addd2bcf41659b7')
pro = ts.pro_api()



def read_factors():
    sql='select * from factors'

    factors=pd.read_sql(sql,engine).set_index('date')
    factors = factors.reset_index()
    factors.rename(columns={'date': 'trade_date'}, inplace=True)
    return factors


def get_tscodes_list():
    sql='select distinct ts_code from company '
    ts_codes=pd.read_sql(sql,engine)
    return ts_codes['ts_code']


#读取某段时间的所有股票的k线，返回日期，收盘价和涨跌幅
def get_stock_lines(start_date,end_date):
    sql='select ts_code,trade_date,pct_chg,close from daily_line where trade_date<=\''+end_date+'\''+'and trade_date>=\''+start_date+'\''
    data=pd.read_sql(sql,engine)
    return data


#获得某只股票涨跌幅数据
def get_stock_pctchg_line(alldata,code):
    df_item=alldata[alldata.ts_code==code]
    df_item=df_item[['trade_date','pct_chg']]
    df_item=df_item.reset_index(drop=True)
    return df_item

#获得某只股票收盘价数据,按时间顺序排列
def get_stock_close_line(alldata,code):
    df_item=alldata[alldata.ts_code==code]
    df_item=df_item[['trade_date','close']]
    df_item=df_item.sort_values(by='trade_date')
    df_item=df_item.reset_index(drop=True)
    return df_item


def get_rf(start_date,end_date):
    sql='select * from bond_yields where date<=\''+end_date+'\''+'and date>=\''+start_date+'\''
    rf=pd.read_sql(sql,engine)
    rf.rename(columns={'date': 'trade_date'}, inplace=True)
    rf = rf[['trade_date', 'oneRate']]
    rf['oneRate'] = rf['oneRate'].map(lambda x: (x / 100 + 1) ** (1 / 360) - 1)
    rf.rename(columns={'oneRate': 'rf'}, inplace=True)
    return rf




def get_ga(startdate,enddate):
    ga= pro.index_daily(ts_code='399317.SZ', start_date=startdate, end_date=enddate)
    ga=ga[['trade_date','pct_chg']]
    ga.rename(columns={'pct_chg':'ga'},inplace=True)
    return ga


# #对某一股票，在某时间段，进行多因子回归
def regression_one_stock(rf,factors,alldata,ga,code):
    #日期默认在20170101之后
    stock_line=get_stock_pctchg_line(alldata,code)
    analy_df=pd.merge(stock_line,ga,on='trade_date')
    analy_df=pd.merge(analy_df,factors,on='trade_date')
    analy_df=pd.merge(analy_df,rf,on='trade_date')
    analy_df['pct_chg']=analy_df['pct_chg']-analy_df['rf']
    analy_df['ga']=analy_df['ga']-analy_df['rf']
    model = sm.OLS(analy_df['pct_chg'], sm.add_constant(
        analy_df[['ga', 'smb', 'hml']].values))
    result = model.fit()
#     print(result.params)
#     print(result.pvalues)
    res_df=pd.DataFrame({'tscode':code,'alpha':result.params['const'],'pvalue':result.pvalues['const']},index=[0])
    return res_df


def get_all_reg(alldata,startdate,enddate):
    factors = read_factors()
    ga=get_ga(startdate,enddate)
    rf=get_rf(startdate,enddate)
    reg_res_list=[]
#     i=0
    tscodes = get_tscodes_list()
    for item in tscodes:
        try:
            reg_df=regression_one_stock(rf,factors,alldata,ga,item)
            reg_res_list.append(reg_df)
        except:
            pass
#         print(str(i)+' done')
#         i=i+1
#     print(reg_res_list)
    return reg_res_list

    
def get_recommand_stocks(alldata,startdate,enddate):
    reg_res=get_all_reg(alldata,startdate,enddate)
    res_df=pd.concat(reg_res)
    res_df=res_df[res_df.alpha>0]
    res_df=res_df[res_df.pvalue<0.05]
    res_df=res_df.sort_values(by='pvalue')
    res_df=res_df.reset_index(drop=True)
    return res_df['tscode'].tolist()


def get_one_returns(alldata,code):
    data=get_stock_close_line(alldata,code)
    dl=data['close'].tolist()
    if len(dl)==0:
        return 0
    return (dl[-1]-dl[0])/dl[0]

def get_all_returns(alldata,code_list,startdate,enddate):
    rf=get_rf(startdate,enddate)
    temp_alldata=pd.merge(alldata,rf,on='trade_date')
    returns_list=[]
    for code in code_list:
        returns=get_one_returns(temp_alldata,code)
        returns_list.append(returns)

    return np.mean(returns_list)

# 缓存回测收益率
def get_backtest_rate():
    year = 2020
    end = datetime.date(year,6,30)
    
    delta_month = datetime.timedelta(days=28)
    delta_week = datetime.timedelta(days=7)
    delta_day = datetime.timedelta(days=1)
    begin = end - delta_month*6- delta_week

    d = begin
    buffer_stock_lines = get_stock_lines(begin.strftime("%Y%m%d"),end.strftime("%Y%m%d"))
    print("--get data--")
    result_list = []
    
    print(begin)
    while (d+delta_month+delta_week)<end:
        d_s = d.strftime("%Y%m%d")
        d_add_m = (d+delta_month-delta_day).strftime("%Y%m%d")
        d_sta = (d+delta_month).strftime("%Y%m%d")
        d_end = (d+delta_month+delta_week).strftime("%Y%m%d")
        
        stock_list = get_recommand_stocks(buffer_stock_lines, d_s,d_add_m)
        rate = get_all_returns(buffer_stock_lines, stock_list,d_add_m,d_end)
        
        print((d_end,rate,stock_list[0:20]))
        result_list.append((d_end,rate,stock_list[0:20]))
        print(",",end="")
        d+=delta_week
    insert_return_rate_list(result_list)

# 根据日期获得大盘基准
def get_back_test_and_base(returns_list):
    df=pd.DataFrame(returns_list)
    df.rename(columns={0: 'date'}, inplace=True)
    df.rename(columns={1: 'model'}, inplace=True)
    shz = pro.index_daily(ts_code='000001.SH', start_date='20170101', end_date='20200830')
    shz=shz[['trade_date','close']]
    shz.rename(columns={'trade_date': 'date'}, inplace=True)
    shz.rename(columns={'close': 'shz'}, inplace=True)
    temp_df=pd.merge(df,shz,on='date')
    date_s=temp_df['date']
    rl=(np.diff(temp_df['shz'])/temp_df['shz'][:-1]).tolist()
    rl.insert(0,0)
    base_ret=pd.DataFrame({'date':date_s,'shz':pd.Series(rl)})
    res=pd.merge(df,base_ret,on='date')
    return res.values.tolist()

# 缓存数据
def get_alter_rate():
    year = 2020
    end = datetime.date(year,2,11)
    
    delta_month = datetime.timedelta(days=28)
    delta_week = datetime.timedelta(days=7)
    delta_day = datetime.timedelta(days=1)
    begin = end - delta_month*6- delta_week

    d = begin
    buffer_stock_lines = get_stock_lines(begin.strftime("%Y%m%d"),end.strftime("%Y%m%d"))
    print("--get data--")
    result_list = []
    
    print(begin)
    while (d+delta_month+delta_week)<end:
        # 用上一个月的因子推这一周的推荐股票
        d_s = d.strftime("%Y%m%d")
        d_add_m = (d+delta_month-delta_day).strftime("%Y%m%d")
        d_sta = (d+delta_month).strftime("%Y%m%d")
        d_end = (d+delta_month+delta_week).strftime("%Y%m%d")
        
        stock_list = get_recommand_stocks(buffer_stock_lines, d_s,d_add_m)
        rate = get_all_returns(buffer_stock_lines, stock_list,d_add_m,d_end)
        
        print((d_end,rate,stock_list[0:20]))
        print(",",end="")
        d+=delta_week
