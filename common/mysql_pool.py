import pymysql
from dbutils.pooled_db import PooledDB
import sys,os
# 同目录文件
common_dir = os.path.dirname(os.path.abspath(__file__))
print("package_path:" + common_dir)
sys.path.append(common_dir)
from common.db_base import *

class MySQLPool:
    def __init__(self, config,maxconnections=10,mincached=1,maxcached=1):
        # self.db_config = db_config
        self.pool = PooledDB(creator=pymysql,
                        maxconnections=maxconnections,  # 连接池允许的最大连接数，0和None表示不限制连接数
                        mincached=mincached,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                        maxcached=maxcached,  # 链接池中最多闲置的链接，0和None不限制
                        maxusage=1,  # 一个链接最多被重复使用的次数，None表示无限制
                        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                        host=config['host'],  # 此处必须是是127.0.0.1
                        port=config['port'],
                        user=config['user'],
                        passwd=config['password'],
                        db=config['database'],
                        use_unicode=True,
                        charset='utf8mb4'
        )
    
    def select(self, sql):
        conn = self.pool.connection()
        try:
            results = exec_select(conn,sql)
        except Exception as e:
            print(e)
        conn.close()
        return results

    def write(self, sqls):
        results = 0
        conn = self.pool.connection()
        try:
            for sql in sqls.split(';'):
                if sql.strip() == '':
                    continue
                results += exec_write_sql(conn,sql) 
        except Exception as e:
            print(e)
        conn.close()
        return results
    
if __name__ == '__main__':
    pm = MySQLPool({
        "host": "172.16.1.61",
        "port": 3306,
        "user": "root",
        "password": "KingPrbt_1",
        "database": "partners"
    })
    sql = """insert into yuyitong_service_prod
(prod_code, prod_name)
values('b0','急啊急啊');
insert into partner_order (`channel`, order_code,status) 
values('bo','2',1);
"""
    rows = pm.write(sql)
    print(rows)