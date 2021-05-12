import dbMan as dbm
import pandas as pd
import toolKit

date_list=['2013-03-29','2013-06-28','2013-09-30', '2013-12-31',
           '2014-03-31','2014-06-30','2014-09-30','2014-12-31',
           '2015-03-31','2015-06-30','2015-09-30','2015-12-31',
           '2016-03-31','2016-06-30','2016-09-30','2016-12-30',
           '2017-03-31','2017-06-30','2017-09-29','2017-12-29',
           '2018-03-30','2018-06-29','2018-09-28','2018-12-28',
           '2019-03-29','2019-06-28','2019-09-30','2019-12-31',
           '2020-03-31','2020-06-30','2020-09-30','2020-12-31',
]
numberOfStock=10
s='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'
engine=dbm.dbConnect(s)

portfolio_value=[]


df_port=pd.DataFrame(columns=['date','code'])
# print(df_port)

# 组合构成表
portfolio=pd.DataFrame(columns=['code','quantity','remaining'])

# 组合总值
value=1000000
total=0




for date in date_list:
    sql='select * from stockIndex where date = \''+date+'\''
    df_rebalance=pd.DataFrame(columns=['date','code','pos'])
    temp=dbm.get_data(engine,sql)
    df_rebalance['date']=temp.iloc[:,0].tolist()
    df_rebalance['code']=temp.iloc[:,1].tolist()
    df_rebalance['pos']=temp.iloc[:,2].tolist()
    # print(df_rebalance)
    df_rebalance.iloc[:,2]=df_rebalance.iloc[:,2].astype(int)
    df_rebalance_sorted=df_rebalance.sort_values(by='pos')
    # print(df_rebalance_sorted)
    df_selected=df_rebalance_sorted.head(numberOfStock)
    # print(df_selected)
    for i in range(0,df_selected.shape[0]):
        sql='select date,code from dataSheet where date = \''+df_selected.iloc[i,0]+'\' and code = \''+df_selected.iloc[i,1]+'\''
        temp=dbm.get_data(engine,sql)
        temp.columns=['date','code']
        # print(temp)
        df_port=df_port.append(temp,ignore_index=False)
        # print(df_port)

# 更新组合

for date in date_list:
    # print(date)
    rec=pd.DataFrame(columns=['code','quantity','remaining'])
    if portfolio.shape[0]==0:
        portfolio_value.append(value)
        dbm.portTrace(engine,date,value)
        print('组合初始化！')
        temp=df_port.loc[df_port['date']==date]
        print('获得当下节点股票列表')
        # print(temp)
        composite=temp.iloc[:,1].tolist()
        # print(composite)
        for i in composite:
            sql='select close from dataSheet where date = \''+date+'\' and code = \''+i+'\''
            temp=dbm.get_data(engine,sql)
            close=float(temp.loc[0,0])
            dbm.portLog(engine,date,'B',i,close)
            print('根据排序，选择购买'+str(i))
            print(str(i)+'的股价为'+str(close))
            # quant=(value/numberOfStock)//close
            # remaining=(value/numberOfStock)%close
            quant,remaining=toolKit.buyStock(numberOfStock,value,close)
            temp1=pd.DataFrame(data=[[i,quant,remaining]],columns=['code','quantity','remaining'])
            rec=rec.append(temp1,ignore_index=False)
            # print(rec)
        portfolio=portfolio.append(rec,ignore_index=False)
        value=0
        #print(portfolio)
    else:
        print('更新持仓市值')
        for i in range(0,numberOfStock):
            code=portfolio.iloc[i,0]
            sql_up='select close from dataSheet where date = \''+date+'\' and code = \''+code+'\''
            temp=dbm.get_data(engine,sql_up)
            new_close=float(temp.loc[0,0])
            dbm.portLog(engine,date,'S',code,new_close)
            value=value+toolKit.updatingValue(portfolio.iloc[i,1],new_close,portfolio.iloc[i,2])
            # value=value+float(portfolio.iloc[i,1])*new_close+float(portfolio.iloc[i,2])
            # print(value)
        print('最新持仓市值为'+str(value))
        portfolio_value.append(value)
        dbm.portTrace(engine,date,value)
        print('更新持仓！')
        temp=df_port.loc[df_port['date']==date]
        # print(temp)
        composite=temp.iloc[:,1].tolist()
        for i in composite:
            sql = 'select close from dataSheet where date = \'' + date + '\' and code = \'' + i + '\''
            temp = dbm.get_data(engine, sql)
            close = float(temp.loc[0, 0])
            dbm.portLog(engine,date,'B',i,close)
            # quant = (value / numberOfStock) // close
            # remaining = (value / numberOfStock) % close
            quant,remaining=toolKit.buyStock(numberOfStock,value,close)
            temp1 = pd.DataFrame(data=[[i, quant, remaining]], columns=['code', 'quantity', 'remaining'])
            rec = rec.append(temp1, ignore_index=False)
            # print(rec)
        portfolio = portfolio.append(rec, ignore_index=False)
        value=0

for i in range(0,numberOfStock):
    code=portfolio.iloc[i,0]
    sql_up='select close from dataSheet where date = \''+str(date_list[-1])+'\' and code = \''+code+'\''
    temp=dbm.get_data(engine,sql_up)
    new_close=float(temp.loc[0,0])
    value=value+toolKit.updatingValue(portfolio.iloc[i,1],new_close,portfolio.iloc[i,2])

print('最终投资组合的总价值为'+str(round(value,2)))
# print(portfolio_value)