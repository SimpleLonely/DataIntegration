#coding=utf-8
from flask import Flask
from flask_apscheduler import APScheduler
from service.schedule import ScheduleConfig
from flask_cors import *
from service import ff_model
from data import return_rate_dao
import datetime

import mysql.connector
import json
app = Flask(__name__)
# import schedule config
app.config.from_object(ScheduleConfig())
CORS(app, resources=r'/*')

mydb = None
db_conf={
    "host" : "119.3.210.244",  # 数据库主机地址
    "user":"root",  # 数据库用户名
    "password":"***",  # 数据库密码
    "database":"data_integration",
}

def db_connect():
    """数据库连接
    根据配置信息,连接数据库
    Arguments:
        conf {dict} -- 连接配置信息
    Returns:
        MySQLConnection -- 数据库连接对象
    Raises:
        e -- 连接报错异常
    """
    global mydb
    global db_conf
    try:
        if isinstance(mydb, mysql.connector.connection.MySQLConnection):
            mydb.ping(True,1,1)
            return mydb
    except Exception as e:
        print('mysql not connected')
 
    try:
        mydb = mysql.connector.connect(**db_conf)
        print('new connect operation')
        return mydb
    except Exception as e:
        raise e

@app.route('/')
def hello_world():
    return 'Hello, W o r l d ! '

@app.route('/getKnowledgeGraph/<code>')
def getKnowledgeGraph(code):
    mycursor = db_connect().cursor()
    mycursor.execute("SELECT * from company where ts_code = %s",(code,))
    company_info =None
    result=mycursor.fetchall()
    if(len(result)>0):
        company_info=result[0]
    if(company_info==None):
        return "null"
    mycursor.execute("SELECT * FROM `manager` where ts_code = %s",(code,))
    manager_info=mycursor.fetchall()
    mycursor.execute("SELECT * FROM `stock_holder` where hold_ts_code = %s",(code,))
    stock_holders=mycursor.fetchall()
    mycursor.execute("SELECT * FROM `hold_fund` where hold_ts_code = %s ",(code,))
    hold_funds=mycursor.fetchall()
    graph={
        "id":company_info[1],
        "children":[
            {
                "id":"省份",
                "children":[
                    {
                        "id":company_info[2]
                    }
                ]
            },
            {
                "id": "行业",
                "children": [
                    {
                        "id": company_info[4]
                    }
                ]
            },
            {
                "id": "板块类型",
                "children": [
                    {
                        "id": company_info[3]
                    }
                ]
            },

            {
                "id": "法人代表",
                "children": [
                    {
                        "id": company_info[6]
                    }
                ]
            },
            {
                "id": "高管",
                "children": []
            },
            {
                "id": "主要股东",
                "children": []
            },
            {
                "id": "持股基金",
                "children": []
            },

        ]
    }
    for manager in manager_info:
        graph["children"][4]["children"].append({
            "id": manager[1],
            "title":manager[2]
        })
    for stock_holder in stock_holders:
        graph["children"][5]["children"].append({
            "id":stock_holder[1],
            "held_ratio":stock_holder[4]
        })
    for hold_fund in hold_funds:
        graph["children"][6]["children"].append({
            "id":hold_fund[1],
            "share_ratio":hold_fund[4]
        })
    return json.dumps(graph,ensure_ascii=False)



# 每周2
@app.route('/return_rate/single')
def get_return_rate():
    begin = datetime.date(2019,10,8)
    end = datetime.date(2020,6,30)
    d = begin
    delta_week = datetime.timedelta(days=7)
    result_list = [((d-delta_week).strftime("%Y%m%d"),0)]
    dao_result = return_rate_dao.get_all_return_rate()
    result_list += dao_result
    
    return json.dumps(ff_model.get_back_test_and_base(result_list),ensure_ascii=False)

@app.route('/getStockCurrentInfo/<code>')
def getStockCurrentInfo(code):
    mycursor = db_connect().cursor()
    mycursor.execute("SELECT * from daily_line where ts_code= %s ORDER BY trade_date desc limit 1",(code,))
    line=None
    result=mycursor.fetchall()
    if(len(result)>0):
        line=result[0]
    else :
        return "null"
    stockInfo={
        "trade_date":line[2],
        "open":line[3],
        "high":line[4],
        "low":line[5],
        "close":line[6],
        "pre_close":line[7],
        "change":line[8],
        "pct_chg":line[9],
        "vol":line[10],
        "amount":line[11]
    }
    trade_date=line[2]
    mycursor.execute("SELECT turnover_rate,pe,pb,dv_ratio,total_mv from daily_base where ts_code= %s and trade_date=%s",(code,trade_date,))
    result=mycursor.fetchall()
    if(len(result)<1):
        return json.dumps(stockInfo,ensure_ascii=False)
    base=result[0]
    stockInfo["turnover_rate"]=base[0]
    stockInfo["pe"]=base[1]
    stockInfo["pb"]=base[2]
    stockInfo["dv_ratio"]=base[3]
    stockInfo["total_mv"]=base[4]
    return json.dumps(stockInfo,ensure_ascii=False)




@app.route('/getRecommendStocks')
def get_recommend_stocks():
    mycursor = db_connect().cursor()
    mycursor.execute("SELECT date,recommend_stocks from alter_returns_rate ORDER BY date desc")
    result = mycursor.fetchall()
    form=[]
    if len(result)<1:
        return "null"
    for line in result:
        item={}
        item["date"]=line[0]
        item["recommendStocks"]=eval(line[1])
        form.append(item)
    return json.dumps(form,ensure_ascii=False)


if __name__ == "__main__":
    scheduler = APScheduler()
    scheduler.init_app(app)
    scheduler.start()
    app.run(host='0.0.0.0',debug=False, port=9000)
