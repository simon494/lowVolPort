import pandas as pd
import numpy as np


def peTTM(df):
    if df.shape[0]!=0:
        return df.iloc[0][5]
    else:
        return -1000.0

def getMomentum(df):
    if df.shape[0]!=0:
        temp = pd.DataFrame(df['close'], dtype=np.float)
        start=temp.iloc[-1][0]
        end=temp.iloc[0][0]
        momentum=end/start
        return momentum
    else:
        return -100

def listPlus(l1,l2):
    result=[]
    if len(l1)==len(l2):
        for i in range(0,len(l1)):
            result.append(l1[i]+l2[i])
        return result
    else:
        print('the length of the list does not match!')

def buyStock(numOfStock,value,close):
    portion=value/numOfStock
    print('每支股票市值上限为'+str(round(portion,2)))
    quant = (value / numOfStock) // close
    remaining = round((value / numOfStock) % close,2)
    print('可够买股票'+str(quant)+'股')
    print('剩余资金'+str(remaining))
    return quant,remaining

def updatingValue(quant,close,remaining):
    # print('更新最新持仓市值')
    return int(quant)*float(close)+float(remaining)