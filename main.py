import baostock as bs
import pandas as pd
import dbMan as dbm
import toolKit as tk

date_list=['2013-03-29','2013-06-28','2013-09-30', '2013-12-31',
           '2014-03-31','2014-06-30','2014-09-30','2014-12-31',
           '2015-03-31','2015-06-30','2015-09-30','2015-12-31',
           '2016-03-31','2016-06-30','2016-09-30','2016-12-30',
           '2017-03-31','2017-06-30','2017-09-29','2017-12-29',
           '2018-03-30','2018-06-29','2018-09-28','2018-12-28',
           '2019-03-29','2019-06-28','2019-09-30','2019-12-31',
           '2020-03-31','2020-06-30','2020-09-30','2020-12-31',
]

s='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'


engine=dbm.dbConnect(s)


lg=bs.login()
# print('login respond error_code:'+lg.error_code)
# print('login respond error_msg:'+lg.error_msg)

rs=bs.query_hs300_stocks()

hs_stocks_list=[]
while(rs.error_code=='0')&rs.next():
    hs_stocks_list.append(rs.get_row_data())
result=pd.DataFrame(hs_stocks_list,columns=rs.fields)
# print(hs_stocks_list)

# sql="select * from dataSheet where date < '2013-03-29' and code = 'sz.300142' limit 750 "
# df=dbm.get_data(conn,sql)
# perMove=tk.calPercentage(df)
# std=tk.stdDev(perMove)
# print(type(std))

stocks_list=[['2021-04-26', 'sh.600000', '浦发银行'],['2021-04-26', 'sh.600004', '白云机场'],['2021-04-26', 'sh.600009', '上海机场'],
             ['2021-04-26', 'sh.600010', '包钢股份'],['2021-04-26', 'sh.600011', '华能国际'], ['2021-04-26', 'sh.600015', '华夏银行'],
             ['2021-04-26', 'sh.600016', '民生银行'], ['2021-04-26', 'sh.600018', '上港集团'], ['2021-04-26', 'sh.600019', '宝钢股份'], ['2021-04-26', 'sh.600025', '华能水电']
]

vol_list=[]
for date in date_list:
    for code in stocks_list:
        # print(date,code[1])
        sql='select * from dataSheet where date <= \''+ date +'\' and code = \''+code[1] +'\' order by date desc limit 750'
        df=dbm.get_data(engine,sql)
        # print(df)
        perMove=tk.calPercentage(df)
        std=tk.stdDev(perMove)
        pe = tk.peTTM(df)
        mom = tk.getMomentum(df)
        vol_list.append([date,code[1],std,pe])
        dbm.insert_data(engine,date,code[1],std,pe,mom)
# df_vol=pd.DataFrame(vol_list)
# print(df_vol)


