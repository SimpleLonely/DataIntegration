import mysql.connector
import time
import pandas as pd

mydb = mysql.connector.connect(
    host="119.3.210.244",  # 数据库主机地址
    user="root",  # 数据库用户名
    passwd="*****",  # 数据库密码
    database="data_integration"
)

# date: yyyymmdd
def get_return_rate_by_date(date:str):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM returns_rate WHERE `date` = %s",(date,))
    myresult = mycursor.fetchone()
    print(myresult)
    return myresult

def insert_return_rate(rate):
    sql = "INSERT INTO  returns_rate (`date`,`rate`) VALUES (%s,%s) "
    mycursor = mydb.cursor()
    mycursor.executemany(sql, rate)
    mydb.commit()
    
    
def insert_return_rate_list(rate_list):
    sql = "INSERT INTO  returns_rate (`date`,`rate`) VALUES (%s,%s) ON DUPLICATE KEY UPDATE `rate`= %s"
    mycursor = mydb.cursor()
    mycursor.executemany(sql, rate_list)
    mydb.commit()


def get_all_return_rate():
    mycursor = mydb.cursor()
    mycursor.execute("SELECT date,rate FROM alter_returns_rate")
    myresult = mycursor.fetchall()
    return myresult

def get_all_date():
    mycursor = mydb.cursor()
    mycursor.execute("SELECT date FROM alter_returns_rate")
    myresult = mycursor.fetchall()
    return myresult

def insert_into_alter_table(rate_list):
    sql = "INSERT INTO alter_returns_rate (date,rate,recommend_stocks) values(%s,%s,%s)"
    mycursor = mydb.cursor()
    mycursor.executemany(sql,rate_list)
    mydb.commit()
  
