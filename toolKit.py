import pandas as pd
import numpy as np

def calPercentage(df):
    if df.shape[0]!=0:
        temp=pd.DataFrame(df[:][3],dtype=np.float)
        traindata=temp.values.tolist()
        list=[]
    # print(traindata[0][0])
        for i in range(1,len(traindata)):
            temp=traindata[i-1][0]
            temp1=traindata[i][0]
            percentage=temp1/temp-1
            list.append(percentage)
        df=pd.DataFrame(list)
    else:
        df=pd.DataFrame(data=None)
    return df

def stdDev(perMove):
    if perMove.shape[0]==0:
        return 1.5
    elif perMove.shape[0]>0 and perMove.shape[0]<=150:
        return np.std(perMove)[0]+1.25
    elif perMove.shape[0]>150 and perMove.shape[0]<=300:
        return np.std(perMove)[0]+1.00
    elif perMove.shape[0]>300 and perMove.shape[0]<=450:
        return np.std(perMove)[0]+0.75
    elif perMove.shape[0]>450 and perMove.shape[0]<=600:
        return np.std(perMove)[0]+0.5
    else:
        return np.std(perMove)[0]

def peTTM(df):
    if df.shape[0]!=0:
        if pd.isnull(df.iloc[0][5]):
            return -1000.0
        else:
            return df.iloc[0][5]
    else:
        return -1000.0

def getMomentum(df):
    # print(df.shape)
    if df.shape[0]!=0:
        temp = pd.DataFrame(df.iloc[:,3], dtype=np.float)
        # print(temp)
        start=temp.iloc[-1][0]
        print('start '+str(start))
        end=temp.iloc[0][0]
        print('end ' + str(end))
        # print(start,end)
        momentum=end/start
        return momentum
    else:
        return -100

def listPlus(l1,l2,w1=1,w2=1):
    result=[]
    if len(l1)==len(l2):
        for i in range(0,len(l1)):
            result.append(l1[i]*w1+l2[i]*w2)
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

def applyWeight(a,weight):
    return a*weight

def stockBaoTo2Share(stock):
    prefix=stock[0:2]
    # print(prefix)
    code=stock[3:9]
    # print(code)
    result=code+'.'+prefix
    return(result.upper())

def stock2ShareToBao(stock):
    code=stock[0:5]
    prefix=stock[7:8]
    result=prefix+'.'+code
    return(result.upper())

def dateBaoTo2Share(date):
    y=date[0:4]
    # print(y)
    m=date[5:7]
    # print(m)
    d=date[8:10]
    # print(d)
    result=y+m+d
    return(result)

def date2SharetoBao(date):
    y =date[0:3]
    m=date[4:5]
    d=date[6:7]
    result=y +'-'+ m +'-'+ d
    return(result)