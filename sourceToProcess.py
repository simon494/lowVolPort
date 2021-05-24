import baostock as bs
import pandas as pd
import numpy as np
import dbMan as dbm
import toolKit
import toolKit as tk
import toolKit as tk
from property import date_list
from property import string
from property import data_source
from property import window
from property import test_date_list
from property import new_date_list

engine=dbm.dbConnect(string)
lg=bs.login()
conn=engine.connect()


process_list=[]
# 将数据载入data_df
sql='select distinct * from '+data_source
data_df=dbm.get_data(engine,sql)
data_df.columns=['index','date','code','close','pct_chg','pe_ttm']
# print(data_df)

# print(data_df[data_df['code']=='601127.SH'])


processed_df=[]
for date in date_list:
    stock_list = []
    # 每一个时间节点获得一次沪深300成分股名单
    # rs = bs.query_hs300_stocks(date)
    # while (rs.error_code == '0') & rs.next():
    #     rec = rs.get_row_data()
    #     stocks_list.append(rec[1])
    # print(stocks_list)
    sql='select code from stockList where date =\''+date+'\''
    temp_list=dbm.get_data(engine,sql).iloc[:,0].tolist()
    for code in temp_list:
        if code not in stock_list:
            stock_list.append(code)
    for code in stock_list:
        print(code)
        # 过滤日期小于季度节点日期数据点，检查是否有超过窗口数的数据条，如果有返回窗口数据量，如果没有返回可查数据
        d=tk.dateBaoTo2Share(date)
        c=code
        # print(c,d)
        temp=data_df[(data_df['date']<=d)&(data_df['code']==c)]
        print(temp)
        index=temp.iloc[0,0]
        end=temp.iloc[-1,0]
        higher=index+window
        print(index,higher)
        print(data_df[(data_df['date']<=d)&(data_df['code']==c)].iloc[0,2])
        print(d)
        if data_df[(data_df['date']<=d)&(data_df['code']==c)].iloc[0,2]==c:
            print('check point')
            if end-index>=window:
                temp=data_df[data_df['code']==c].iloc[index:higher,:]
                if temp.shape[0] != 0:
                    per = temp.iloc[:, 4].astype(float)
                    vol = np.std(per)
            else:
                temp = data_df[(data_df['date']<=d)&(data_df['code']==c)]
                if temp.shape[0] != 0:
                    per = temp.iloc[:, 4].astype(float)
                    vol = np.std(per)
        else:
            vol=100.0
        print("std: "+str(vol))
        pe = tk.peTTM(temp)
        print("pe: "+str(pe))
        momentum = tk.getMomentum(temp)
        print("mom: "+str(momentum))
        process_list.append([date,code,vol,pe,momentum])
    print(len(process_list))

result=pd.DataFrame(process_list,columns=['date','code','vol','pe','momentum'])
result.to_sql('processData',conn,if_exists='append')