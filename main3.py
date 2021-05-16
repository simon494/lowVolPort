import baostock as bs
import pandas as pd
import numpy as np
import dbMan as dbm
import toolKit2 as tk
from property import date_list
from property import string
from property import data_source

engine=dbm.dbConnect(string)
lg=bs.login()
stocks_list=[]
vol_list=[]

for date in date_list:
    rs = bs.query_hs300_stocks(date)
    while (rs.error_code == '0') & rs.next():
        rec = rs.get_row_data()
        stocks_list.append(rec[1])
    # print(stocks_list)
    for code in stocks_list:
        # print(date,code[1])
        sql='select distinct * from '+data_source+' where date <= \''+ date +'\' and code = \''+code +'\' order by date desc limit 750'
        df=dbm.get_data(engine,sql)
        # print(df)
        per=df.iloc[:,4].astype(float)
        std=np.std(per)
        # print(std)
        pe = tk.peTTM(df)
        # print(pe)
        mom = tk.getMomentum(df)
        # print(mom)
        vol_list.append([date,code,std,pe])
        insert_sql = 'insert into processData2 (date,code,vol,pe,momentum) values (\'' + date + '\',\'' + code + '\',\'' + str(std) + '\',\'' + str(pe) + '\',\'' + str(mom) + '\')'
        print(insert_sql)
        dbm.insert_data(engine,'processData2',date,code,insert_sql)



