import baostock as bs
import toolKit
import pandas as pd
import dbMan as man
from property import date_list
from property import string



lg=bs.login()
print('login respond error_code:'+lg.error_code)
print('login respond error_msg:'+lg.error_msg)
engine=man.dbConnect(string)
conn=engine.connect()

df = pd.DataFrame()
for date in date_list:
    print(date)
    stock_list = []
    st = bs.query_hs300_stocks(date)
    while(st.error_code=='0')&st.next():
        rec=st.get_row_data()
        code=toolKit.stockBaoTo2Share(rec[1])
        stock_list.append([date,code])
    df=df.append(stock_list)
df.columns=['date','code']
df.to_sql('stockList', conn, if_exists='append')
# print(df)