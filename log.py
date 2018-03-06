import logging
import datetime
import os

logpath = 'logs/'
logname = 'logs/'+datetime.datetime.now().strftime('%Y-%m-%d')+'.log'
loglevel = 1
logger = ''

format_dict = {
   1 : logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
   2 : logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
   3 : logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
   4 : logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
   5 : logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
}

#开发一个日志系统， 既要把日志输出到控制台， 还要写入日志文件  
class Logger():
    #def __init__(self, logname, loglevel, logger):
    def __init__(self):    
        '''
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        '''

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        if os.path.exists(logname):
            print("日志文件已存在")
        else:
            if os.path.exists(logpath):
                print("日志文件路径已存在")
            else:                
                os.mkdir(logpath)  #创建目录
                
            open(logname,"w+").close() #创建空文件
            #os.mknod(logname)        
            
        fh = logging.FileHandler(logname)
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = format_dict[int(loglevel)]
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

   
    def getlog(self):
        return self.logger
