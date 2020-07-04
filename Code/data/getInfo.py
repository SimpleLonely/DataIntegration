import tushare as ts
import mysql.connector
import time
import pandas as pd


ts.set_token('ed713a2e27e3930b752d0638476753e267a66c1a6addd2bcf41659b7')
mydb = mysql.connector.connect(
    host="119.3.210.244",  # 数据库主机地址
    user="root",  # 数据库用户名
    passwd="*****",  # 数据库密码
    database="data_integration"
)


def get_daily_lines(start_date, end_date):
    pro = ts.pro_api()
    sql = "INSERT INTO  daily_line (`ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`,`change`,`pct_chg`,`vol`,`amount`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    date_list = pro.trade_cal(exchange='SSE', is_open='1',
                              start_date=start_date,
                              end_date=end_date,
                              fields='cal_date').values.tolist()
    print("get_daily_lines:"+str(len(date_list))+"个交易日")
    for date in date_list:
        tuplelist = get_daily_line(date[0])
        insert_data(sql, tuplelist)
        print(date[0]+" 成功插入日线数据")
    return

# call daily
def get_daily_line(date):
    pro = ts.pro_api()
    for _ in range(3):
        try:
            df = pro.daily(trade_date=date)
            list = df.values.tolist()
            tuplelist = convert_listlist2tuplelist(list)
        except:
            time.sleep(1)
        else:
            return tuplelist
    print(date+" 获取日线数据失败---------------------------------------")
    return []


def get_daily_bases(start_date, end_date):
    pro = ts.pro_api()
    sql = "INSERT INTO  daily_base (`ts_code`,`trade_date`,`close`,`turnover_rate`,`turnover_rate_f`,`volume_ratio`,`pe`,`pe_ttm`,`pb`,`ps`,`ps_ttm`,`dv_ratio`,`dv_ttm`,`total_share`,`float_share`,`free_share`,`total_mv`,`circ_mv`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    date_list = pro.trade_cal(exchange='SSE', is_open='1',
                              start_date=start_date,
                              end_date=end_date,
                              fields='cal_date').values.tolist()
    print("get_daily_bases:"+str(len(date_list))+"个交易日")
    for date in date_list:
        tuplelist = get_daily_base(date[0])
        insert_data(sql, tuplelist)
        print(date[0]+" 成功插入每日指标数据")
    return

# call daily
def get_daily_base(date):
    pro = ts.pro_api()
    for _ in range(3):
        try:
            df = pro.daily_basic(trade_date=date)
            df = df.astype(object).where(pd.notnull(df), None)
            list = df.values.tolist()
            tuplelist = convert_listlist2tuplelist(list)
        except:
            time.sleep(1)
        else:
            return tuplelist
    print(date+" 获取每日指标失败---------------------------------------")
    return []


def insert_data(sql, data):
    mycursor = mydb.cursor()
    mycursor.executemany(sql, data)
    mydb.commit()


def convert_listlist2tuplelist(param):
    result = []
    for item in param:
        tuple = ()
        for i in item:
            tuple = tuple+(i,)
        item = tuple
        result.append(item)
    return result


def get_fina_indicators(start_date, end_date):
    sql = "INSERT INTO  fina_indicator (ts_code,ann_date,end_date,fa_turn,inv_turn,assets_turn,ca_turn,debt_to_assets,current_ratio,roe,q_eps,profit_to_gr,q_profit_yoy,basic_eps_yoy,eqt_to_debt,tangibleasset_to_debt,fcff,q_npta,op_of_gr,or_yoy,q_gc_to_gr) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    stock_list = get_stock_code_list()
    for code in stock_list:
        tuplelist = get_fina_indicator(start_date, end_date, code)
        insert_data(sql, tuplelist)
        print(start_date+" "+end_date+" "+code + " 获取财务指标成功")


def get_fina_indicator(start_date, end_date, code):
    pro = ts.pro_api()
    for i in range(15):
        try:
            df = pro.fina_indicator(ts_code=code, start_date=start_date, end_date=end_date,
                                    fields='ts_code,ann_date,end_date,fa_turn,inv_turn,assets_turn,ca_turn,debt_to_assets,current_ratio,roe,q_eps,profit_to_gr,q_profit_yoy,basic_eps_yoy,eqt_to_debt,tangibleasset_to_debt,fcff,q_npta,op_of_gr,or_yoy,q_gc_to_gr')
            df = df.astype(object).where(pd.notnull(df), None)
            list = df.values.tolist()
            tuplelist = convert_listlist2tuplelist(list)
        except:
            print("重试")
            time.sleep(10)
        else:
            return tuplelist
    print(start_date+" "+end_date+" "+code +
          " 获取财务指标失败---------------------------------------")
    return []


def get_stock_code_list():
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L',
                           fields='ts_code').values.tolist()
    return [item[0] for item in data]


if __name__ == "__main__":

    # get_daily_lines("20170101","20200629")
    # get_daily_bases("20170101","20200629")
    # get_fina_indicators("20170101","20200629 ")
    pass

