import baostock as bs
import pandas as pd
import dbMan as man
from property import date_list
from property import string
from property import data_source
from property import start_date
from property import end_date

lg=bs.login()
print('login respond error_code:'+lg.error_code)
print('login respond error_msg:'+lg.error_msg)

engine=man.dbConnect(string)
conn=engine.connect()
stock_list=[]

for date in date_list:
    st = bs.query_hs300_stocks(date)
    while(st.error_code=='0')&st.next():
        rec=st.get_row_data()
        code=rec[1]
        if code not in stock_list:
            stock_list.append(code)

for code in stock_list:
    data_list = []
    rs = bs.query_history_k_data_plus(code, "date,code,close,pctChg,peTTM", start_date, end_date,'d', '2')
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result.to_sql(data_source, conn, if_exists='append')

conn.close()
bs.logout()