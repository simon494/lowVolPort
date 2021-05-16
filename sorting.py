import pandas as pd
import dbMan as dbm
import toolKit as tk
from property import string
from property import date_list
from property import vol_weight
from property import pe_weight
from property import mom_weight

# engine=dbm.dbConnect(s)
engine=dbm.dbConnect(string)


for date in date_list:
    # 对vol进行排序
    vol_sql='select date,code,vol from processData2 where date = \''+date+'\''
    df_vol=dbm.get_data(engine,vol_sql)
    # print(type(df_vol))
    df_vol.iloc[:,2]=df_vol.iloc[:,2].astype(float)
    df_vol_sorted=df_vol.sort_values(by=2,ascending=True)
    df_vol_sorted[3]=list(range(1,df_vol_sorted.shape[0]+1))
                     # *vol_weight
    df_vol_sorted.columns=['date','code','vol','vol_index']
    # print(df_vol_sorted)

    # 对PE进行排序
    pe_sql='select date,code,pe from processData2 where date = \''+date+'\''
    df_pe=dbm.get_data(engine,pe_sql)
    df_pe.loc[df_pe[2]=='',2]=1000
    df_pe.iloc[:,2]=df_pe.iloc[:,2].astype(float)
    # print(df_pe)
    df_pe.loc[df_pe[2]<=0,2]=1000
    # print(df_pe)
    df_pe_sorted=df_pe.sort_values(by=2,ascending=True)
    df_pe_sorted[3]=list(range(1,df_vol_sorted.shape[0]+1))
                    # *pe_weight
    df_pe_sorted.columns=['date','code','pe','pe_index']
    # print(df_pe_sorted)

    # 对Momentum进行排序
    mon_sql='select date,code,momentum from processData2 where date = \''+date+'\''
    df_mom=dbm.get_data(engine,mon_sql)
    # print(df_mom)
    df_mom.iloc[:,2]=df_mom.iloc[:,2].astype(float)
    df_mom_sorted=df_mom.sort_values(by=2,ascending=False)
    # print(df_mom_sorted)
    df_mom_sorted[3]=list(range(1,df_mom_sorted.shape[0]+1))
    df_mom_sorted.columns=['date','code','momentum','mom_index']
    # print(df_mom_sorted)

    # 三表汇总并写入数据库
    temp=pd.merge(pd.merge(df_vol_sorted,df_pe_sorted,on=['date','code']),df_mom_sorted,on=['date','code'])
    df_combine=pd.DataFrame(columns=['date','code','rating'])
    df_combine['date']=temp['date'].tolist()
    df_combine['code']=temp['code'].tolist()
    temp1=tk.listPlus(temp['vol_index'],temp['pe_index'],vol_weight,pe_weight)
    df_combine['rating']=tk.listPlus(temp1,temp['mom_index'].tolist(),1,mom_weight)
    # print(df_combine.iloc[0][0])
    for i in range(0,df_combine.shape[0]):
        insert_sql='insert into stockIndex ( date, code, pos) values ( \''+df_combine.iloc[i][0]+'\',\''+df_combine.iloc[i][1]+'\',\''+str(df_combine.iloc[i][2])+'\')'
        dbm.insert_data(engine,'stockIndex',df_combine.iloc[i][0],df_combine.iloc[i][1],insert_sql)