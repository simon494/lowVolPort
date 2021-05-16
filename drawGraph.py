import baostock as bs
import matplotlib.pyplot as plt
import pandas as pd
import dbMan as dbm
from property import date_list
from property import string
engine=dbm.dbConnect(string)

lg=bs.login()
# print('login respond error_code:'+lg.error_code)
# print('login respond  error_msg:'+lg.error_msg)
data_list = []

for date in date_list:
    rs = bs.query_history_k_data_plus(code='sh.000300',fields='date,close',start_date=date,end_date=date,frequency='d',adjustflag='2')
    # print('query_history_k_data_plus respond error_code:'+rs.error_code)
    # print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)
    while(rs.error_code=='0')&rs.next():
        data_list.append(rs.get_row_data())
hs_index=pd.DataFrame(data_list)
hs_index.columns=['date','hs300']
# print(hs_index)

port_sql='select * from portTrace order by date'
port_value=dbm.get_data(engine,port_sql)
port_value.columns=['date','portfolio_Value']
new=pd.merge(port_value,hs_index,on='date')
new['portfolio_Value']=new['portfolio_Value'].astype(float)/1000000
new['hs300']=new['hs300'].astype(float)/2495
# print(new)

plt.figure(figsize=(15,7))
plt.plot(new['date'],new['portfolio_Value'],label='portfolio')
plt.plot(new['date'],new['hs300'],color='r',label='HS300')
plt.ylabel('Performance')
plt.xticks(rotation=-90)
plt.tick_params(axis='x',labelsize=7)
plt.legend()
plt.title('Performance comparison')
plt.show()



# plt.scatter(date_list,port)
# plt.show()
