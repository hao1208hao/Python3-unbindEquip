from flask import Flask,jsonify,request
 
#from log import * 
from DBUtil import *

log = Logger().getlog()

app = Flask(__name__)#创建一个服务，赋值给APP

   

#########更新费率#########
@app.route('/updateUpgradeCodeType',methods = ['GET', 'POST'])
def updateUpgradeCodeType():
    if request.method == 'POST':
        agentNo = request.form.get('agentNo') 
        upgradeType = request.form.get('upgradeType')
        flag = request.form.get('flag')    # 0 查询升级码类别对应的费率        1   要更新为新的升级码类别
    elif request.method == 'GET': 
        agentNo = request.args.get('agentNo')
        upgradeType = request.args.get('upgradeType')
        flag = request.args.get('flag') 
    else:
        agentNo = request.args.get('agentNo')
        upgradeType = request.args.get('upgradeType')
        flag = request.args.get('flag')

    
    mssql = DBUtil('sqlserver')    
    
    #查询费率信息
    if flag=="0":
        if  not upgradeType:
            log.info('查询升级码类别为空,请重新请求。')
            return jsonify({'retCode':'-99','retMsg':'unvalid upgradeCode! please request Again!'})
            #return jsonify({'retCode':'-99','retMsg':'升级码类别不能为空,请重新请求。'})
            #return  {'retCode':'-99','retMsg':'升级码类别不能为空,请重新请求。'}
        
        sql = '''SELECT TOP 1 CAST(交易费率 AS varchar(50)) rate,交易费率封顶值 rateLimit,结算策略组 settleType,
                CAST(加急费率 AS varchar(50)) quickRate,加急封顶 quickRateLimit
                FROM code.升级码_类别 WHERE 升级码类别='%s' AND 注册子渠道='大财神' ORDER BY 记录时间 DESC'''
        
        data = (upgradeType)      
        resp = mssql.ExecQuery(sql % data)        
        
        if not resp:
            log.info('获取到的升级码类别是:【'+upgradeType+'】,未查到该升级码类别对应的费率信息。')
            return jsonify({'retCode':'-96','retMsg':'has no this upgradeCode!'})
            #return jsonify({'retCode':'-96','retMsg':'未查到该升级码类别对应的费率信息。'})
            #return {'retCode':'-96','retMsg':'未查到该升级码类别对应的费率信息。'}

         
        
        orderRate = resp[0]['rate']
        rateInfo1 = "获取到的升级码类别是:【"+upgradeType+"】,查询相关信息:\n交易费率:【"+orderRate+"】,交易封顶金额：【"+str(resp[0]['rateLimit'])+"】"
        rateInfo2 = ",结算周期：【"+resp[0]['settleType']+"】,\n加急费率：【"+str(resp[0]['quickRate'])+"】,加急封顶金额：【"+str(resp[0]['quickRateLimit'])+"】"
        rateInfo = rateInfo1+rateInfo2
        log.info(rateInfo)
        return jsonify({'retCode':'00','retMsg':'query success!','codeType':upgradeType,'fees':orderRate,'feesCap':resp[0]['rateLimit'],'settleTime':resp[0]['settleType'],'urgentRate':resp[0]['quickRate'],'urgentRateCap':resp[0]['quickRateLimit']})
        #return {'retCode':'00','retMsg':'查询成功','codeType':upgradeType,'fees':orderRate,'feesCap':resp[0]['rateLimit'],'settleTime':resp[0]['settleType'],'urgentRate':resp[0]['quickRate'],'urgentRateCap':resp[0]['quickRateLimit']}
        
    
    #更新费率
    if not agentNo or not upgradeType:
        log.info('商户编号或者升级码类别为空,请重新请求。')
        #return jsonify({'retCode':'-99','retMsg':'商户编号或者升级码类别不能为空,请重新请求。'})
        return jsonify({'retCode':'-99','retMsg':'agentNo or upgradeCode can not empty!'})
        #return {'retCode':'-99','retMsg':'商户编号或者升级码类别不能为空,请重新请求。'}

    
    sql2 = "SELECT TOP 1 升级码类别 codeType,升级码 code  FROM [code].[升级码_经销商库存表] WITH(NOLOCK) WHERE 状态='已使用' and 升级码使用商户编号='%s'"
    data2 = (agentNo)
    resp2 = mssql.ExecQuery(sql2 % data2)
    if not resp2:
        log.info('获取到的商户编号是:【'+agentNo+'】,未查到该商户的升级码使用记录。')
        return jsonify({'retCode':'-98','retMsg':'has no use records for this agent'})
        #return jsonify({'retCode':'-98','retMsg':'未查到该商户的升级码使用记录。'})
        #return {'retCode':'-98','retMsg':'未查到该商户的升级码使用记录。'}


    log.info("获取到的商户编号是:【"+agentNo+"】,修改前的升级码是：【"+resp2[0]['code']+"】,修改前的升级码类别为：【"+resp2[0]['codeType']+"】,修改后的升级码类别为：【"+upgradeType+"】。")

    sql3 = "SELECT 升级码类别 codeType FROM agent.商户信息表 a WITH(NOLOCK),[code].[升级码_类别]  c WITH(NOLOCK) WHERE a.来源子渠道=c.注册子渠道 AND 是否删除=0 AND 商户编号='%s' AND 升级码类别='%s' "
    data3 = (agentNo,upgradeType)
    resp3 = mssql.ExecQuery(sql3 % data3)
    if not resp3:        
        log.info("该商户编号:【"+agentNo+"】暂不支持此【"+upgradeType+"】升级码类别。")
        return jsonify({'retCode':'-97','retMsg':'unsupport this upgradeCode type:【'+upgradeType+'】 '})
        #return jsonify({'retCode':'-97','retMsg':'暂不支持此【'+upgradeType+'】升级码类别。'})
        #return {'retCode':'-97','retMsg':'暂不支持此【'+upgradeType+'】升级码类别。'}

    
    sql4 = "UPDATE TOP(1) [code].[升级码_经销商库存表] SET 升级码类别='%s' WHERE 状态='已使用' and 升级码使用商户编号='%s'"
    data4 = (upgradeType,agentNo)
    rowcount = mssql.ExecNonQuery(sql4 % data4)
    if rowcount == 1:            
        log.info("商户编号【"+agentNo+"】对应的升级码类别更新成功。")
        return jsonify({'retCode':'00','retMsg':'ragte update success!'})
        #return jsonify({'retCode':'00','retMsg':'费率更新成功'})
        #return {'retCode':'00','retMsg':'费率更新成功'}
    else:
        log.info("商户编号【"+agentNo+"】对应的升级码类别更新失败。")
        #return jsonify({'retCode':'-1','retMsg':'费率更新失败'})
        return jsonify({'retCode':'00','retMsg':'ragte update fail!'})
        #return {'retCode':'-1','retMsg':'费率更新失败'}
         
    

#########解绑设备#########
@app.route('/unbindEqno',methods = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE'])#指定接口访问的路径，支持什么请求方式get，post    
def unbind():
    if request.method == 'POST':
        agentNo = request.form.get('agentNo')#获取接口请求中form-data的username参数传入的值
        eqno = request.form.get('eqno')#获取接口请求中form-data的username参数传入的值
    elif request.method == 'GET': 
        agentNo = request.args.get('agentNo')
        eqno = request.args.get('eqno')
    else:
        agentNo = request.args.get('agentNo')
        eqno = request.args.get('eqno')

 
    if  not agentNo  and not eqno:
        log.info('没有获取到商户编号,并且没有获取到设备编号,请重新请求')        
        return jsonify({'retCode':'-99','retMsg':'has no agentNo ,and has no eqno!'})
        #return jsonify({'retCode':'-99','retMsg':'没有获取到商户编号,也没有获取到设备编号'})
        #return {'retCode':'-99','retMsg':'没有获取到商户编号,也没有获取到设备编号'}
    

    #if len(agentNo)>0 and len(eqno)>0:
    if  agentNo and  eqno:
        log.info("获取到的商户编号是:【"+agentNo+"】;设备编号是:【"+eqno+"】");        
        sql = "select top 10 标识 id,插入时间 insertTime,设备编号 eqno,商户编号 agentNo,备注 remark,设备类型 equipType from [agent].[商户_绑定_设备] WHERE 商户编号='%s' and 设备编号='%s' "
        data = (agentNo,eqno)
    elif agentNo and not eqno:
        log.info("获取到的商户编号是:【"+agentNo+"】");       
        sql = "select top 10 标识 id,插入时间 insertTime,设备编号 eqno,商户编号 agentNo,备注 remark,设备类型 equipType from [agent].[商户_绑定_设备] WHERE 商户编号='%s'"
        data = (agentNo)
    elif eqno and not agentNo:
        log.info("获取到的设备编号是:【"+eqno+"】");       
        sql = "select top 10 标识 id,插入时间 insertTime,设备编号 eqno,商户编号 agentNo,备注 remark,设备类型 equipType from [agent].[商户_绑定_设备] WHERE 设备编号='%s'"
        data = (eqno)
 
    

    mssql = DBUtil('sqlserver')    
    resp = mssql.ExecQuery(sql % data)  
     

    if not resp:
        if  agentNo and  eqno:
            log.info("未查询到商户【"+agentNo+"】绑定设备【"+eqno+"】记录")               
        elif agentNo and not eqno:
            log.info("该商户【"+agentNo+"】暂未查询到有绑定设备记录")          
        elif eqno and not agentNo:
            log.info("该设备【"+eqno+"】暂未查询到有绑定记录")           

        return  jsonify({'retCode':'-98','retMsg':'has no this agent records or eqno bind log' })
        #return  jsonify({'retCode':'-98','retMsg':'未查询到该商户或设备有绑定设备记录' })
        #return  {'retCode':'-98','retMsg':'未查询到该商户或设备有绑定设备记录' }   


    updateFlag = True


    for i in range(len(resp)):        
        log.info("该商户【"+resp[i]['agentNo']+"】绑定的设备有:"+resp[i]['eqno']);       

        sql = "UPDATE TOP(1) [agent].[商户_绑定_设备] SET 设备编号=设备编号+'_del' WHERE 设备编号='%s' "
        data2 = (resp[i]['eqno'])
        rowcount = mssql.ExecNonQuery(sql % data2)
        if rowcount == 1:            
            log.info("设备编号【"+resp[i]['eqno']+"】更新成功")            
        else:
            log.info("设备编号【"+resp[i]['eqno']+"】更新失败")            
            updateFlag = False

    if not  updateFlag:
        return  jsonify({'retCode':'-1','retMsg':'设备解绑失败' })
        #return  jsonify({'retCode':'-1','retMsg':'eqno unbind fail!' })
        #return  {'retCode':'-1','retMsg':'设备解绑失败' }
 
    return  jsonify({'retCode':'00','retMsg':'eqno unbind success!' })
    #return  jsonify({'retCode':'00','retMsg':'设备解绑成功' })
    #return  {'retCode':'00','retMsg':'设备解绑成功' }

 



app.run(host='0.0.0.0',port=8802,debug=True)

#访问方式    localhost:8802/unbindEqno?eqno=abc
