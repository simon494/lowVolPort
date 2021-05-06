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
    conn=engine.connect()
    print('Database connected!')
    return conn

def get_data(conn,sql):
    data_list=[]
    try:
        with conn:
            print('Executing SQL!')
            cur=conn.execute(sql)
    except:
        print('Exception raised')
        cur=None
    for row in cur:
        #print(row)
        data_list.append(row)
    df=pandas.DataFrame(data_list)
    return df

