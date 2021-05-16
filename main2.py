import baostock as bs
import dbMan as dbm
import toolKit as tk
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
    print(stocks_list)
    for code in stocks_list:
        # print(date,code[1])
        sql='select * from '+data_source+' where date <= \''+ date +'\' and code = \''+code +'\' order by date desc limit 750'
        df=dbm.get_data(engine,sql)
        # print(df)
        perMove=tk.calPercentage(df)
        std=tk.stdDev(perMove)
        pe = tk.peTTM(df)
        mom = tk.getMomentum(df)
        vol_list.append([date,code,std,pe])
        insert_sql = 'insert into processData (date,code,vol,pe,momentum) values (\'' + date + '\',\'' + code + '\',\'' + str(
            std) + '\',\'' + str(pe) + '\',\'' + str(mom) + '\')'
        dbm.insert_data(engine,'processData',date,code,insert_sql)



