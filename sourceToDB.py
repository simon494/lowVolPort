import baostock as bs
import tushare as ts
import pandas as pd

import dbMan
import dbMan as man
import toolKit
from property import date_list
from property import string
from property import data_source
from property import start_date
from property import end_date
from property import test_stock_list
import time

# baostock api 获得股票数据

engine=man.dbConnect(string)
conn=engine.connect()

# tushare 获得股票数据
ts.set_token('452d0532266a04c3d77dde58ad53e187f89168b025df527003594167')
pro=ts.pro_api()


stock_list=[]
for date in date_list:
    # st = bs.query_hs300_stocks(date)
    # while(st.error_code=='0')&st.next():
    #     rec=st.get_row_data()
    #     code=toolKit.stockBaoTo2Share(rec[1])
    #     if code not in stock_list:
    #         stock_list.append(code)
    sql='select code from stockList where date =\''+date+'\''
    temp_list=dbMan.get_data(engine,sql).iloc[:,0].tolist()
    for code in temp_list:
        if code not in stock_list:
            stock_list.append(code)


    # print(temp_list)

print(stock_list)
# print(stock_list_fixed)

temp=pd.DataFrame()

for code in stock_list:
    data_list=[]
    # print(code)
    t1=pro.daily(ts_code=code,start_date=start_date,end_date=end_date)
    df1=t1[['trade_date','ts_code','close','pct_chg']]
    t2=pro.daily_basic(ts_code=code,start_date=start_date,end_date=end_date,field='trade_date,ts_code,close,pe_ttm')
    # print(t2)
    df2=t2[['trade_date','ts_code','close','pe_ttm']]
    # print(df2)
    df=pd.DataFrame.merge(df1,df2,on=['trade_date','ts_code','close'])
    print(df)
    # temp=temp.append(df)
    df.to_sql(data_source, conn, if_exists='append')
    time.sleep(45)











#
# for code in stock_list:
#     data_list = []
#     rs = bs.query_history_k_data_plus(code, "date,code,close,pctChg,peTTM", start_date, end_date,'d', '2')
#     print('query_history_k_data_plus respond error_code:' + rs.error_code)
#     print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
#     while (rs.error_code == '0') & rs.next():
#         data_list.append(rs.get_row_data())
#     result = pd.DataFrame(data_list, columns=rs.fields)
#     result.to_sql(data_source, conn, if_exists='append')
#
# conn.close()
# bs.logout()