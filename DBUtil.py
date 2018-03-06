#import pymssql
import pyodbc
import pymysql.cursors

from log import *
import json

'''
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER='+host+';DATABASE='+database+';UID='+user+';PWD='+password+'')
cur = conn.cursor()
if not cur:
    raise (NameError,"数据库连接失败")
cur.execute("SELECT top 1 * from dealer.dealers with(nolock)")
resList = cur.fetchall()
#fetchall()是接收全部的返回结果行
conn.close()
print(resList)

'''

host = ""
port = ""
user = ""
password = ""
database = ""


class DBUtil():
    def __init__(self,databaseType):
        '''
        if databaseType == 'sqlserver': 
            self.host = host+','+port
        elif  databaseType =='mysql':
            self.host = host
            self.port = port
        '''
        self.host = host
        self.port = port    
        self.user = user
        self.password = password
        self.database = database
        #self.log = Logger().getlog()
        self.databaseType = databaseType


    # 数据库类型  'mysql'  'sqlserver'
    def __GetConnect(self):
        
        if hasattr(self,'conn'):
            return self.cursor
        else:
            if self.databaseType == 'sqlserver':
                self.host = self.host+','+self.port
                self.conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER='+self.host+';DATABASE='+self.database+';UID='+self.user+';PWD='+self.password)
            elif self.databaseType =='mysql':
                self.conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, password=self.password, db=self.database,charset='utf8')
                
            self.cursor = self.conn.cursor()
            if not self.cursor:
                raise(NameError,"数据库连接失败")
            else:
                return self.cursor
	     		

	 
     
    def ExecQuery(self, sql):
        cursor = self.__GetConnect()
        
        cursor.execute(sql)
        resList = cursor.fetchall()
        
        self.resList = resList
	# 获取MYSQL里面的数据字段名称
        self.fields = cursor.description
        data = []  
        if self.resList:                  
            dictArr = {}
            for index in range(len(self.resList)):
                valueStr = ''
                for i in range (len(self.resList[index])):
                    key = self.fields[i][0]
                    value = str(self.resList[index][i])                     
                    dictStr='"'+key+'":"'+value+'"'                    
                    valueStr = valueStr+dictStr+','
                     
                strLen = len(valueStr)-1                
                valueStr =  valueStr[0:strLen]
                valueStr = '{'+valueStr+'}'
                jsonStr = json.loads(valueStr)
                data.insert(index,jsonStr)            
          
        # 查询完毕后必须关闭连接
        #cursor.close()
        #self.conn.close()
        return data


    def ExecNonQuery(self, sql):
        cursor = self.__GetConnect()
        cursor.execute(sql)
        self.conn.commit()
	#self.cursor.close()
        #self.conn.close()
        return cursor.rowcount


    #执行存储过程
    def ExecProedure(self, procedureName,data,flag=0):
        cursor = self.__GetConnect()
        #参考链接：http://blog.csdn.net/samed/article/details/49963531
        #参数说明：  1、存储过程名称、2、存储过程参数名称,类型 (例：param1,param2,%d,%s) 3、 存储过程参数值

        #flag 0 表示查影响行数,  1 查询列表   默认为0
        if flag == 0:
            cursor.execute('EXEC '+procedureName+ data)
            data = cursor.rowcount
             
            #print(cursor.rowcount)       #可以得到存储过程影响行数,不关心返回数据
        else:            
            resultList = cursor.execute('SET NOCOUNT ON; EXEC '+procedureName+ data).fetchall()
            #print(resultList);      数据列表

            self.resList = resList
            # 获取MYSQL里面的数据字段名称
            self.fields = cursor.description
            data = []  
            if self.resList:                  
                dictArr = {}
                for index in range(len(self.resList)):
                    valueStr = ''
                    for i in range (len(self.resList[index])):
                        key = self.fields[i][0]
                        value = self.resList[index][i]              
                        dictStr='"'+key+'":"'+value+'"'                    
                        valueStr = valueStr+dictStr+','
                     
                        #valueStr = valueStr+jsonStr+','
                        #print(valueStr)

                    strLen = len(valueStr)-1                
                    valueStr =  valueStr[0:strLen]
                    valueStr = '{'+valueStr+'}'
                    jsonStr = json.loads(valueStr)
                    data.insert(index,valueStr)  
                
             
        self.conn.commit()
	#self.cursor.close()
        #self.conn.close()
        return data

    def closeConn(self):
        self.cursor.close()
        self.conn.close()
        
 
if __name__ == '__main__':
    a = 'wf55'
    beginData = '2017-11-01'
    queryAgentNo = '981818696638'
    data =  (queryAgentNo,beginData)   
    #sql = ''' SELECT 订单号 FROM [order].订单信息表_异常 WHERE 用户编号 = '%s' AND 记录时间> '%s'     '''

     
    sql = ''' SELECT TOP 1 CAST(交易费率 AS varchar(50)) rate,交易费率封顶值 rateLimit,结算策略组 settleType,
        CAST(加急费率 AS varchar(50)) quickRate,加急封顶 quickRateLimit
        FROM code.升级码_类别 WHERE 升级码类别='wf55' AND 注册子渠道='大财神' ORDER BY 记录时间 DESC
        '''
            
    result = DBUtil('sqlserver').ExecQuery(sql )
    if not result:
        print("没有数据")
    print("=====================")
    print(result)

    '''
    for i in range(len(result)):
        orderNo = result[i]
        print('查询到订单号：'+orderNo['订单号'])
    '''    
         
