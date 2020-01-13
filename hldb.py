# coding: utf-8

import pandas as pd
import numpy  as np
import cx_Oracle
import pymysql
import pyodbc
import configparser
from openpyxl import load_workbook

import os
import time
import codecs
import getpass
import zipfile
import tarfile
import ftplib as FTP


class hldb:
    '''
    A class to help with massive data
    Support read data from oracle/mysql/hadoop to Dataframe
    Support export with csv/excel 
    Support ftp operator
    Support zip/tar operator
    '''
    def load_sql(self, path, args='', needCheck=False):
        '''
        use placeholders to load sql from sql file
        use param needCheck=True to show total sql
        '''
        if ((len(path) < 5) or (path[-4:] != '.sql')):
            print('sql文件必须以.sql结尾')
            return ''
        rootDir = os.getcwd()
        sqlFilePath = os.path.join(rootDir, path)
        f = codecs.open(sqlFilePath, 'r', encoding='utf-8')
        sqls = f.readlines()
        sql = ''
        if (len(sqls) == 0):
            print('sql文件为空')
            return ''
        else:
            for line in sqls:
                sql += line
        if (args != ''):
            sql = sql.format(**args)
        if (needCheck):
            print(sql)
        simiIndex = sql.find(';')
        if ((simiIndex > -1) and (simiIndex < len(sql) - 1) and (sql.find(';', simiIndex + 1) > -1)):
            print('存在两个以上的结束符;')
            return ''
        sqlUpper = sql.upper()
        msg = ''
        if (sqlUpper.find('CREATE ')) > -1:
            msg = 'create'
        elif (sqlUpper.find('COMMIT')) > -1:
            msg = 'commit'
        if (msg != ''):
            msg = 'sql中含有 ' + msg + ' ，不能继续执行:'
            print(msg)
            return ''
        sql = sql.replace(';','')
        return sql
    def load_sqls(self, path, args='', needCheck=False):
        '''
        use placeholders list to load sqls list from one sql file
        use param needCheck=True to show total sql list
        '''
        if (args == ''):
            return load_sql(path=path, needCheck=needCheck)
        length = -1
        sqls = []
        argsList = []
        for key in args:
            value = args[key]
            if (type(value) != list):
                print('传入参数必须是列表')
                return ''
            if (length == -1):
                for i in range(len(value)):
                    argsList.append({})
                length = len(value)
            elif (len(value) != length):
                print('传入参数异常')
                return ''
            for i in range(len(argsList)):
                argsList[i][key] = args[key][i]
        for args in argsList:
            sql = self.load_sql(path=path, args=args, needCheck=False)
            sqls.append(sql)
        if (needCheck):
            for sql in sqls:
                print(sql)
        return sqls
    def connect(self, needPassword=False, needUser=False ,connToBigDB=False ,connToMysql=False):
        '''
        default read user/password from ini 
        modify param let read user/password from interactive(needPassword=True, needUser=True)
        
        default param       connect To ORACLE
        connToMysql=TRue    connect To MYSQL
        connToBigDB=TRue    connect To HADOOP
        '''
        #oracle
        global oracle_user
        global oracle_password
        global mysql_user
        global mysql_password
        global impala_user
        global impala_password
        
        global myconfig
        global myconfigFilePath
        rootDir = os.getcwd()
        myconfigFilePath = os.path.join(rootDir, 'hldb.ini')
        myconfig = configparser.ConfigParser()
        myconfig.read(myconfigFilePath, encoding='utf-8')
        
        #首先判断连什么库
        if (connToMysql==False)&(connToBigDB==False):
            #然后判断读从哪里读配置
            if (needUser==True):
                oracle_user = input('oracle_user:')
            if (needUser==False):
                oracle_user = myconfig.get('conf', 'oracle_user')
            if (needPassword==True):
                oracle_password = getpass.getpass('oracle_password:')
            if (needPassword==False):
                oracle_password = myconfig.get('conf', 'oracle_password')
            #然后从配置读取基础配置信息
            oracle_dbip = myconfig.get('conf', 'oracle_dbip')
            oracle_dbname = myconfig.get('conf', 'oracle_dbname')
            
            db = oracle_user + '/' + oracle_password + '@' + oracle_dbip + ':1521/' + oracle_dbname
            #最后建立连接
            self.__conn = cx_Oracle.connect(db + '', encoding = 'UTF-8', nencoding='UTF-8')
            self.__cursor = self.__conn.cursor()
            print('oracle connected')
            
        elif (connToMysql==True)&(connToBigDB==False):
            if (needUser==True):
                mysql_user = input('mysql_user:')
            if (needUser==False):
                mysql_user = myconfig.get('conf', 'mysql_user')
            if (needPassword==True):
                mysql_password = getpass.getpass('mysql_password:')
            if (needPassword==False):
                mysql_password = myconfig.get('conf', 'mysql_password')
                
            mysql_host = myconfig.get('conf', 'mysql_host')
            mysql_port = int(myconfig.get('conf', 'mysql_port'))
            mysql_database = myconfig.get('conf', 'mysql_database')
            
            self.__conn = pymysql.connect(host=mysql_host,port=mysql_port,database=mysql_database,charset='utf8',user=mysql_user,password=mysql_password)
            self.__cursor = self.__conn.cursor()
            print('mysql connected')
        elif (connToMysql==False)&(connToBigDB==True):
            if (needUser==True):
                impala_user = input('impala_user:')
            if (needUser==False):
                impala_user = myconfig.get('conf', 'impala_user')
            if (needPassword==True):
                impala_password = getpass.getpass('impala_password:')
            if (needPassword==False):
                impala_password = myconfig.get('conf', 'impala_password')
                
            impala_host = myconfig.get('conf', 'impala_host')
            impala_port = myconfig.get('conf', 'impala_port')
            impala_driver_name = myconfig.get('conf', 'impala_driver_name')
            
            self.__conn = self.__connImpala(ip=impala_host, port=impala_port, user=impala_user, password=impala_password,impala_driver_name=impala_driver_name)
            self.__cursor = self.__conn.cursor()
            print('hadoop connected')
        elif (connToMysql==True)&(connToBigDB==True):
            print('一个实例不能同时连接多个数据库,请检查输入条件')
        else:
            print('异常条件')
    def __connImpala(self, ip=None, port=None, user=None, password=None,impala_driver_name=None):
        driverParas = 'DRIVER={};Host={};Port={};AuthMech=3;UID={};PWD={}'.format(impala_driver_name, ip, port, user,password)
        conn = pyodbc.connect(driverParas, autocommit=True)
        return conn        
    def read_sql(self, sql, fileprefix='', filesuffix='', sheetname=''):
        '''
        read data from db with sql,return dataframe
        fileprefix is datafile's name before '.'
        filesuffix is 'csv' or 'xlsx',not 'xls'
        default data filesuffix is 'csv'
        sheetname only used when filesuffix='xlsx'
        '''
        return self.__read_sql(sql,fileprefix=fileprefix, filesuffix=filesuffix, sheetname=sheetname)
    def __read_sql(self, sql, fileprefix='', filesuffix='', sheetname='', batchsuffix=''):
        sql = sql.replace(';','')
        starttime = time.time()
        arr = pd.read_sql(sql, self.__conn)
        print('sql executed time : ' + format(time.time() - starttime, '0.3f') + 's')
        df = pd.DataFrame(arr)
        timestamp = time.strftime('%Y-%m-%d %H-%M-%S',time.localtime())
        filename=''
        if (fileprefix != ''):
            filename = fileprefix
        #batchsuffix:read_sqls批量生成文件的时候用以区分生成的文件名
        filename += batchsuffix
        if (not (fileprefix == '' and filesuffix == '' and sheetname == '' and batchsuffix == '')):
            if (filesuffix == 'xlsx' and fileprefix != ''):
                if (sheetname == ''):
                    sheetname = 'sheet1'
                filename += '.xlsx'
                if (os.path.exists(filename)):
                    book = load_workbook(filename)
                    sheets = book.sheetnames
                    if (sheetname in sheets):
                        idx = sheets.index(sheetname)
                        book.remove(book.worksheets[idx])
                    writer = pd.ExcelWriter(filename, engine='openpyxl')
                    writer.book = book
                    df.to_excel(writer, sheetname)
                    writer.save()
                else:
                    writer = pd.ExcelWriter(filename)
                    df.to_excel(writer, sheetname)
                    writer.save()
                self.__modify_columns_width(df, filename, sheetname)
            else :
                df.to_csv(filename + '.csv',index=False, encoding='utf_8_sig')                
        return df
    def __modify_columns_width(self, df, filename, sheetname):
        wb = load_workbook(filename)
        ws_list = wb.sheetnames
        for i in range(len(ws_list)):
            ws = wb[ws_list[i]]
            if (ws_list[i] != sheetname):
                continue
            df_len = df.apply(lambda x:[(len(str(i).encode('utf-8')) - len(str(i))) / 2 + len(str(i)) for i in x], axis = 0)
            df_len_max = df_len.apply(lambda x:max(x), axis=0)
            j = list(df.columns)
            j.insert(0, '')
            for i in df.columns:
                column_letter = [chr(j.index(i) + 65) if j.index(i) <= 25 else 'A'+chr(j.index(i) - 26 + 65)][0]
                columns_length = (len(str(i).encode('utf-8')) - len(str(i))) / 2 + len(str(i))
                data_max_length = df_len_max[i]
                column_width = [data_max_length if columns_length < data_max_length else columns_length][0]
                column_width = [column_width if column_width > 15 else 15][0]
                ws.column_dimensions['{}'.format(column_letter)].width = column_width
        wb.save(filename=filename)
    def read_sqls(self, sqls, fileprefix='', filesuffix='', sheetname=''):
        dfs = []
        batchsuffix = ''
        isFileCreated = not (fileprefix == '' and filesuffix == '' and sheetname == '')
        for i in range(len(sqls)):
            if (isFileCreated):
                batchsuffix = '_'+str(1+i)
            dfs.append(self.__read_sql(sqls[i],fileprefix=fileprefix,filesuffix=filesuffix,sheetname=sheetname,batchsuffix=batchsuffix))
        return dfs    
    def ftp_update(self,remotefilepath,localfilename, needUser=False, needPassword=False):
        '''
        used for update local file to ftp's remotefilepath
        
        default read user/password from ini 
        modify param let read user/password from interactive(needPassword=True, needUser=True)
        '''
        global ftp_username
        global ftp_password
        global myconfig
        global myconfigFilePath
        
        rootDir = os.getcwd()
        myconfigFilePath = os.path.join(rootDir, 'hldb.ini')
        myconfig = configparser.ConfigParser()
        myconfig.read(myconfigFilePath, encoding='utf-8')
        if (needUser):
            ftp_username = input('ftp_user:')
        else:
            ftp_username = myconfig.get('conf', 'ftp_user')
            
        if (needPassword):
            ftp_password = getpass.getpass('ftp_password:')
        else:
            ftp_password = myconfig.get('conf', 'ftp_password')
        ftp_ip = myconfig.get('conf', 'ftp_ip')
        ftp_port = myconfig.get('conf', 'ftp_port')
        bufsize = myconfig.get('conf', 'ftp_bufsize')
        
        ftp=FTP.FTP()
        ftp.encoding = myconfig.get('conf', 'ftp_encoding')
        ftp.connect(ftp_ip,ftp_port)
        ftp.login(ftp_username,ftp_password)
        ftp.cwd(remotefilepath)
        file=open(localfilename,'rb')
        ftp.storbinary('STOR '+localfilename,file,bufsize)
        file.close()
        print("上传后FTP目录下文件为: ")
        ftp.dir()
        ftp.quit()
    def ftp_download(self,remotefilepath,remotefilename,localfilename, needUser=False, needPassword=False):
        '''
        used for download remote file on ftp to local file
        
        default read user/password from ini 
        modify param let read user/password from interactive(needPassword=True, needUser=True)        
        '''
        global ftp_username
        global ftp_password
        global myconfig
        global myconfigFilePath
        ftp_port = myconfig.get('conf', 'ftp_port')
        bufsize = myconfig.get('conf', 'ftp_bufsize')
        rootDir = os.getcwd()
        myconfigFilePath = os.path.join(rootDir, 'hldb.ini')
        myconfig = configparser.ConfigParser()
        myconfig.read(myconfigFilePath, encoding='utf-8')
        if (needUser):
            ftp_username = input('ftp_user:')
        else:
            ftp_username = myconfig.get('conf', 'ftp_user')
        if (needPassword):
            ftp_password = getpass.getpass('ftp_password:')
        else:
            ftp_password = myconfig.get('conf', 'ftp_password')
            
        ftp_ip = myconfig.get('conf', 'ftp_ip')
        ftp=FTP.FTP()
        ftp.encoding = myconfig.get('conf', 'ftp_encoding')
        ftp.connect(ftp_ip,ftp_port)
        
        ftp.login(ftp_username,ftp_password)
        ftp.cwd(remotefilepath)
        file=open(localfilename,'wb')
        ftp.retrbinary('RETR '+remotefilename,file.write,bufsize)
        file.close()
        ftp.quit()
        print("下载后当前目录文件：")
        print(os.listdir())
    def close(self):
        '''
        Don't forget to use close() at last!
        use it to close connect to db
        and clear any plainetxt password in hldb.ini
        '''
        self.__conn.close()
        print('hldb disconnected')
        myconfig.set('conf', 'oracle_password', '')
        myconfig.set('conf', 'ftp_password', '')
        myconfig.set('conf', 'impala_password', '')
        myconfig.set('conf', 'mysql_password', '')
        myconfig.write(open(myconfigFilePath, 'w'))
        print('password is clear')
    def gen_ini(self):
        '''
        Generate a profile in the current directory for next
        please modify it for your env
        '''
        Isgen=True
        if os.path.exists('hldb.ini'):
            print("hldb.ini 已经存在，是否需要覆盖? y/n")
            if (str.upper(input()) != 'Y'):
                Isgen=False
        if Isgen==True:
            config = configparser.ConfigParser()
            config['conf'] = {
                            'oracle_dbip' : '172.x.x.x',
                            'oracle_dbname' : '',
                            'oracle_user' : '',
                            'oracle_password' : '',
                            'ftp_username' : '',
                            'ftp_password' : '',
                            'ftp_ip' : '172.x.x.x',
                            'ftp_port' : '21',
                            'ftp_bufsize' : '1024',
                            'impala_host' : '172.x.x.x',
                            'impala_port' : '21050',
                            'impala_user' : '',
                            'impala_password' : '',
                            'impala_driver_name' : 'Cloudera ODBC Driver for Impala',
                            'mysql_host' : '172.x.x.x',
                            'mysql_port' : '3306',
                            'mysql_database' : '',
                            'mysql_user' : '',
                            'mysql_password' : ''
                             }
            with open('hldb.ini', 'w') as configfile:
                config.write(configfile)
            print("已在当前目录下生成配置文件hldb.ini,请修改该文件以便与您的环境匹配")
    def compress_file(self,src_name,compress_name,mode='zip'):
        '''
        compress a file to 'zip' or 'tar' on param
        '''
        if ((src_name=='') or (compress_name=='')):
            print("请输入待压缩或压缩后的文件名或目录名")
            return
        if  os.path.exists(src_name) != True:
            print("待压缩文件不存在")
            return
        if mode=='zip':
            compress_name+='.zip'
            if os.path.isfile(src_name):
                with zipfile.ZipFile(compress_name, 'w') as z:
                    z.write(src_name)
            else:
                with zipfile.ZipFile(compress_name, 'w') as z:
                    for root, dirs, files in os.walk(src_name):
                        for single_file in files:
                            if single_file != compress_name:
                                filepath = os.path.join(root, single_file)
                                z.write(filepath)
                z.close()
        elif mode=='tar':
            compress_name+='.tar'
            if os.path.isfile(src_name):
                with tarfile.open(compress_name, 'w') as tar:
                    tar.add(src_name)
            else:
                with tarfile.open(compress_name, 'w') as tar:
                    for root, dirs, files in os.walk(src_name):
                        for single_file in files:
                            if single_file != compress_name:
                                filepath = os.path.join(root, single_file)
                                tar.add(filepath)
                #z.close()
        else:
            print("目前仅支持zip/tar压缩方式")
        
        return compress_name