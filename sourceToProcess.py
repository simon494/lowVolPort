import baostock as bs
import pandas as pd
import numpy as np
import dbMan as dbm
import toolKit as tk
from property import date_list
from property import string
from property import data_source
from property import window
from property import test_date_list

engine=dbm.dbConnect(string)
lg=bs.login()
process_list=[]
conn=engine.connect()

# 将数据载入data_df
sql='select distinct * from '+data_source
data_df=dbm.get_data(engine,sql)
data_df.columns=['index','date','code','close','perChg','pe']

# print(data_df[data_df['code']=='sh.601127'])


processed_df=[]
for date in date_list:
    stocks_list = []
    # 每一个时间节点获得一次沪深300成分股名单
    rs = bs.query_hs300_stocks(date)
    while (rs.error_code == '0') & rs.next():
        rec = rs.get_row_data()
        stocks_list.append(rec[1])
    # print(stocks_list)
    for code in stocks_list:
        # 过滤日期小于季度节点日期数据点，检查是否有超过窗口数的数据条，如果有返回窗口数据量，如果没有返回可查数据
        temp=data_df[(data_df['date']<=date)&(data_df['code']==code)]
        index=temp.iloc[-1,0]
        lower=index-window
        if lower>0:
            temp=data_df[data_df['code']==code].iloc[lower:index,:]
        else:
            temp = data_df[data_df['code'] == code].iloc[0:index,:]

        per = temp.iloc[:, 4].astype(float)
        vol = np.std(per)
        # print("std: "+str(std))
        pe = tk.peTTM(temp)
        # print("pe: "+str(pe))
        momentum = tk.getMomentum(temp)
        # print("mom: "+str(mom))
        process_list.append([date,code,vol,pe,momentum])
    print(len(process_list))
result=pd.DataFrame(process_list,columns=['date','code','vol','pe','momentum'])

result.to_sql('processData',conn,if_exists='append')