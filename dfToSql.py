import baostock as bs
import pandas as pd
import sqlalchemy as al
import dbMan as man

lg=bs.login()
print('login respond error_code:'+lg.error_code)
print('login respond error_msg:'+lg.error_msg)

st=bs.query_hs300_stocks()
s='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'
engine=man.dbConnect(s)
conn=engine.connect()

data_list = []
while(st.error_code=='0')&st.next():
    print(st.get_row_data()[1])
    rs = bs.query_history_k_data_plus(st.get_row_data()[1], "date,code,close,peTTM", '2010-01-01', '2021-01-01', 'd','3')
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())


result = pd.DataFrame(data_list, columns=rs.fields)
result.to_sql('dataSheet', conn, if_exists='append')

conn.close()
bs.logout()