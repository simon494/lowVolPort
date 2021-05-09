import pymysql
import sqlalchemy as al
import pandas

# class dbMan():
#     def __init__(self):
#         self.host='localhost'
#         self.user='root'
#         self.passwd='123456'
#         self.string='mysql+pymysql://root:123456@localhost/lowVolPort?charset=utf8'
#         print('Database initialized!')
#
#
#     def dbConnect(self,string):
#         # conn=pymysql.connect(
#         #     host=self.host,
#         #     user=self.user,
#         #     passwd=self.passwd,
#         #     database=db,
#         #     cursorclass=pymysql.cursors.DictCursor
#         # )
#         engine=al.create_engine(string,
#                                 echo=True,
#                                 pool_size=8,
#                                 pool_recycle=60*30)
#         conn=engine.connect()
#         print('Database connected!')
#         return conn

    # def dbExecute(self,db,sql):
    #     conn=self.dbConnect(db)
    #     curs=conn.cursor()
    #     res=curs.execute(sql)
    #     if(res):
    #         print(curs.fetchall())
    #         return curs.fetchall()

# db=pymysql.connect(
#     host='localhost',
#     user='root',
#     passwd='css860210',
#     database='lowVolPort')
# cur=db.cursor()
# res=cur.execute('show tables')
# print(res)
# db.close()
def dbConnect(string):
    engine=al.create_engine(string,
                            echo=True,
                            pool_size=8,
                            pool_recycle=60*30)
    print('Database connected!')
    return engine

def get_data(engine,sql):
    data_list=[]
    try:
        with engine.connect() as conn:
            print('Executing SQL! '+sql)
            cur = conn.execute(sql)
            for row in cur:
                # print(row)
                data_list.append(row)
    except:
        print('Exception raised')


    df=pandas.DataFrame(data_list)
    return df

def insert_data(engine,date,code,vol=None,pe=None,momentum=None):
    print('Insert data')
    try:
        with engine.connect() as conn:
            cur=conn.execute(
                'select * from processData where date = \''+ date +'\' and code = \''+ code + '\''
            )
            res=cur.fetchone()

            if res!=None:
                print('data exist!')
            else:
                print('inserting new data!')
                insert_sql = 'insert into processData (date,code,vol,pe,momentum) values (\'' + date + '\',\'' + code + '\',\'' + str(vol) + '\',\'' + str(pe) +'\',\''+str(momentum)+'\')'
                print(insert_sql)
                with engine.connect() as conn:
                    conn.execute(insert_sql)
    except:
        print('Insert exception occured!')