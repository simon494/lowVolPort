import baostock as bs
import pandas as pd
import dbMan as dbm

date_list=['2013-03-29', '2013-06-28', '2013-09-30', '2013-12-31',
           '2014-03-31','2014-06-30','2014-09-30','2014-12-31',
           '2015-03-31','2015-06-30','2015-09-30','2015-12-31'
]

s='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'


conn=dbm.dbConnect(s)


lg=bs.login()
print('login respond error_code:'+lg.error_code)
print('login respond error_msg:'+lg.error_msg)

rs=bs.query_hs300_stocks()

hs_stocks_list=[]
while(rs.error_code=='0')&rs.next():
    hs_stocks_list.append(rs.get_row_data())
result=pd.DataFrame(hs_stocks_list,columns=rs.fields)
# print(hs_stocks_list[0][1])

# sql="select * from dataSheet where date < '2013-03-29' and code = 'sz.300142' limit 750 "
# df=dbm.get_data(conn,sql)

for date in date_list:
    for code in hs_stocks_list:
        # print(date,code[1])
        sql='select * from dataSheet where date <'+ date +' and code ='+code[1] +' limit 750'
        print(sql)