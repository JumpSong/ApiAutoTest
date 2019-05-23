# -*- coding: UTF-8 -*-
import pymysql
import logging
import os
from configparser import ConfigParser

class Op_Database(object):

    def __init__(self,dbName):
        cp = ConfigParser()
        cp.read("conf.ini")
        section = cp.sections()[0]
        host = cp.get(section,"host")
        user = cp.get(section,"user")
        passwd = cp.get(section,"passwd")
        port = cp.getint(section,"port")
        #初始化类
        self.conn = pymysql.connect(db = dbName,host=host,user=user,passwd=passwd,port=port,charset="utf8")
        self.cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    #操作数据库
    def op_sql(self,param):
        try:
            self.cur.execute(param)
            self.conn.commit()
            return True
        except pymysql.Error as e:
            print("Mysql Error %d:%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
            return False

    #查询一条记录
    def selectOne(self,sql):
        results = []
        try:
            self.cur.execute(sql)
            results = self.cur.fetchone()
        except pymysql.Error as e:
            results = "执行sql错误"
            print("Mysql Error %d：%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
        finally:
            return results

    #查询全部记录
    def selectAll(self,sql):
        results = []
        try:
            self.cur.execute(sql)
            self.cur.scroll(0,mode='absolute')
            results = self.cur.fetchall()
        except pymysql.Error as e:
            results = "执行sql错误"
            print("Mysql Error %d：%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
        finally:
            return results

    #插入数据
    def insert(self,sql,param):
        try:
            self.cur.execute(sql,param)
            self.conn.commit()
            return True
        except pymysql.Error as e:
            print("Mysql Error %d：%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
            return False

    #删除数据
    def delete(self,sql,param):
        try:
            self.cur.executemany(sql,param)
            self.conn.commit()
            return True
        except pymysql.Error as e:
            print("Mysql Error %d：%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
            return False

    #重建数据库,慎用
    def restruct_db(self,dbName):
        try:
            result = self.selectAll("SELECT CONCAT('TRUNCATE TABLE ',TABLE_NAME,';') FROM information_schema.TABLES WHERE TABLE_SCHEMA='{0}'".format(dbName))
            self.cur.execute("SET FOREIGN_KEY_CHECKS=0")
            self.conn.commit()
            for i in range(len(result)):
                 for val in result[i].values():
                     if val != None:
                         self.cur.execute(val)
                         self.conn.commit()
            self.cur.execute("SET FOREIGN_KEY_CHECKS=1")
            self.conn.commit()
            return True
        except pymysql.Error as e:
            print("Mysql Error %d:%s" % (e.args[0], e.args[1]))
            logging.basicConfig(filename=os.path.join(os.getcwd(), './mysql_errlog.txt'),
                                level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(e)
            return False

    def __del__(self):
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()





if __name__ == "__main__":
    db = Op_Database("httprunner")
    sql = db.restruct_db('httprunner')
    print(sql)