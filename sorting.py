import pandas as pd
import dbMan as dbm
import toolKit as tk

s='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'
engine=dbm.dbConnect(s)

date_list=['2013-03-29','2013-06-28','2013-09-30', '2013-12-31',
           '2014-03-31','2014-06-30','2014-09-30','2014-12-31',
           '2015-03-31','2015-06-30','2015-09-30','2015-12-31',
           '2016-03-31','2016-06-30','2016-09-30','2016-12-30',
           '2017-03-31','2017-06-30','2017-09-29','2017-12-29',
           '2018-03-30','2018-06-29','2018-09-28','2018-12-28',
           '2019-03-29','2019-06-28','2019-09-30','2019-12-31',
           '2020-03-31','2020-06-30','2020-09-30','2020-12-31',
]


for date in date_list:
    # 对vol进行排序
    vol_sql='select date,code,vol from processData where date = \''+date+'\''
    df_vol=dbm.get_data(engine,vol_sql)
    # print(type(df_vol))
    df_vol.iloc[:,2]=df_vol.iloc[:,2].astype(float)
    df_vol_sorted=df_vol.sort_values(by=2,ascending=True)
    df_vol_sorted[3]=list(range(1,df_vol_sorted.shape[0]+1))
    df_vol_sorted.columns=['date','code','vol','vol_index']
    # print(df_vol_sorted)

    # 对PE进行排序
    pe_sql='select date,code,pe from processData where date = \''+date+'\''
    df_pe=dbm.get_data(engine,pe_sql)
    df_pe.iloc[:,2]=df_pe.iloc[:,2].astype(float)
    # print(df_pe)
    df_pe.loc[df_pe[2]<=0,2]=1000
    # print(df_pe)
    df_pe_sorted=df_pe.sort_values(by=2,ascending=True)
    df_pe_sorted[3]=list(range(1,df_vol_sorted.shape[0]+1))
    df_pe_sorted.columns=['date','code','pe','pe_index']
    # print(df_pe_sorted)

    # 对Momentum进行排序
    mon_sql='select date,code,momentum from processData where date = \''+date+'\''
    df_mom=dbm.get_data(engine,mon_sql)
    # print(df_mom)
    df_mom.iloc[:,2]=df_mom.iloc[:,2].astype(float)
    df_mom_sorted=df_mom.sort_values(by=2,ascending=False)
    # print(df_mom_sorted)
    df_mom_sorted[3]=list(range(1,df_mom_sorted.shape[0]+1))
    df_mom_sorted.columns=['date','code','momentum','mom_index']
    # print(df_mom_sorted)

    # 三表汇总并写入数据库
    # df_combine=pd.DataFrame(columns=['date','code','rating'])
    # df_combine['date']=df_vol.iloc[:,0].tolist()
    # df_combine['code']=df_vol.iloc[:,1].tolist()
    # print(df_vol_sorted.dtypes)
    temp=pd.merge(pd.merge(df_vol_sorted,df_pe_sorted,on=['date','code']),df_mom_sorted,on=['date','code'])
    # print(temp)
    df_combine=pd.DataFrame(columns=['date','code','rating'])
    df_combine['date']=temp['date'].tolist()
    df_combine['code']=temp['code'].tolist()
    temp1=tk.listPlus(temp['vol_index'],temp['pe_index'])
    df_combine['rating']=tk.listPlus(temp1,temp['mom_index'].tolist())
    # print(df_combine.iloc[0][0])
    for i in range(0,df_combine.shape[0]):
        insert_sql='insert into stockIndex ( date, code, pos) values ( \''+df_combine.iloc[i][0]+'\',\''+df_combine.iloc[i][1]+'\',\''+str(df_combine.iloc[i][2])+'\')'
        dbm.insert_data(engine,'stockIndex',df_combine.iloc[i][0],df_combine.iloc[i][1],insert_sql)