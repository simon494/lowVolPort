import baostock as bs
import pandas as pd
import dbMan as dbm
from venv.lib import toolKit as tk
from property import date_list
from property import string


engine=dbm.dbConnect(string)
lg=bs.login()
# print('login respond error_code:'+lg.error_code)
# print('login respond error_msg:'+lg.error_msg)

rs=bs.query_hs300_stocks()

hs_stocks_list=[]
while(rs.error_code=='0')&rs.next():
    hs_stocks_list.append(rs.get_row_data())
result=pd.DataFrame(hs_stocks_list,columns=rs.fields)

stocks_list=[]
for i in range(100):
    stocks_list.append(hs_stocks_list[i])

print(len(stocks_list))

vol_list=[]
for date in date_list:
    for code in hs_stocks_list:
        # print(date,code[1])
        sql='select * from dataSheet where date <= \''+ date +'\' and code = \''+code[1] +'\' order by date desc limit 750'
        df=dbm.get_data(engine,sql)
        # print(df)
        perMove=tk.calPercentage(df)
        std=tk.stdDev(perMove)
        pe = tk.peTTM(df)
        mom = tk.getMomentum(df)
        vol_list.append([date,code[1],std,pe])
        insert_sql = 'insert into processData (date,code,vol,pe,momentum) values (\'' + date + '\',\'' + code[1] + '\',\'' + str(
            std) + '\',\'' + str(pe) + '\',\'' + str(mom) + '\')'
        dbm.insert_data(engine,'processData',date,code[1],insert_sql)



