import os,sys
# 同目录文件
common_dir = os.path.dirname(os.path.abspath(__file__))
print("package_path:" + common_dir)
sys.path.append(common_dir)
from db_base import exec_select, exec_write_sql


class CRUDBase:
    def __init__(self, tablename, model, conn=None, dbname='',camelcase=False, verbose=1):
        self.table_name = tablename
        self.model = model
        self.verbose = verbose
        self.conn = conn
        self.db_name = dbname
        self.camelcase = camelcase

    def to_columns(self, data):
        """转换用户输入字段名到数据库字段名"""
        if not self.camelcase:# 非驼峰法，不转换
            return data
        return data

    def to_api(self, data):
        """转换数据库字段名到用户输入字段名"""
        if not self.camelcase:# 非驼峰法，不转换
            return data
        return data

    def fields_vals(self, data):
        """转换用户输入字段名到数据库字段名"""
        fields, vals = [], []
        data = self.to_columns(data)
        for key, value in self.model.items():
            if value["create"] <= 0:
                continue
            fields.append(key)
            if key in data:
                if isinstance(data[key], str):
                    vals.append(f"'{data[key]}'")
                else:
                    vals.append(f"{data[key]}")
            else:
                vals.append(f"{value['default']}")
        return fields, vals

    def create(self, data, conn=None):
        """创建一条记录"""
        if conn is None and self.conn is None:
            return None
        fields, vals = self.fields_vals(data)
        sql = f"""insert into {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} ({','.join(fields)}) values ({','.join(vals)})"""
        print(sql)
        return exec_write_sql(self.conn if conn is None else conn, sql)

    def update_items(self, data):
        """
        转换用户输入字段名
        update 没有默认值，所以需要手动指定
        """
        vals = []
        data = self.to_columns(data)
        for key, value in self.model.items():
            if value["update"] <= 0:
                continue
            # fields.append(key)
            if key in data and data[key] is not None:
                if isinstance(data[key], str):
                    vals.append(f"{key}='{data[key]}'")
                else:
                    vals.append(f"{key}={data[key]}")
        return vals

    def update(self, data, conn=None):
        """更新一条记录"""
        if conn is None and self.conn is None:
            return None
        if 'id' not in data:
            return None
        rows = self.update_items(data)
        sql = f"""update {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} set 
        {','.join(rows)} 
        where id = {data['id']}"""
        if self.verbose > 1:
            print(sql)
        return exec_write_sql(self.conn if conn is None else conn, sql)

    def get(self, id, conn=None):
        if conn is None and self.conn is None:
            return None
        sql = f"select * from {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} where id = {id}"
        return exec_select(self.conn if conn is None else conn, sql)

    def change_status(self, id, status, conn=None):
        if conn is None and self.conn is None:
            return None
        sql = f"update {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} set status = {status} where id = {id}"
        return exec_write_sql(self.conn if conn is None else conn, sql)

    def query_items(self, data):
        """查询用的字段名转换"""
        vals = []
        data = self.to_columns(data)
        for key, value in self.model.items():
            if value["query"] <= 0:
                continue
            if key in data and data[key] is not None:
                if isinstance(data[key], str):
                    vals.append(f"instr({key},'{data[key]}')")
                else:
                    vals.append(f"{key}={data[key]}")

        return vals

    def page(self, data, conn=None):
        if conn is None and self.conn is None:
            return None
        offset = 0 if ('page' not in data or 'page_size' not in data) else data["page"] * data["page_size"]
        size=100 if 'page_size' not in data else data['page_size']
        rows = self.query_items(data)
        where = '' if len(rows) == 0 else f"where {' and '.join(rows)}"
        sql0 = f"select * from {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} {where} limit {offset}, {size}"
        sql1 = f"select count(1) cnt from {'' if self.db_name == '' else self.db_name+'.'}{self.table_name} {where}"
        if self.verbose > 1:
            print(sql0)
        # 获取总数
        total = exec_select(self.conn if conn is None else conn, sql1)[0]['cnt']
        # 获取分页数据
        rows = exec_select(self.conn if conn is None else conn, sql0)
        return total, rows


if __name__ == "__main__":
    import pymysql
    from dbutils.pooled_db import PooledDB

    pool = PooledDB(
        creator=pymysql,
        maxconnections=1,  # 连接池允许的最大连接数，0和None表示不限制连接数
        mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
        maxusage=1,  # 一个链接最多被重复使用的次数，None表示无限制
        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        host="172.16.1.61",  # 此处必须是是127.0.0.1
        port=3306,
        user="root",
        passwd="KingPrbt_1",
        db="partners",
        use_unicode=True,
        charset="utf8mb4",
    )
    c = CRUDBase(
        "yuyitong_area",
        {
            "id": {
                "create": 0,
                "read": False,
                "update": False,
                "query": True,
                "default": 0,
            },
            "area_code": {
                "create": 2,  # 0=False,1=True,2=auto random
                "read": True,
                "update": True,
                "query": True,
                "default": "'0'",
            },
            "area_name": {
                "create": 1,
                "read": True,
                "update": True,
                "query": True,
                "default": 0,
            },
            "level": {
                "create": 1,
                "read": True,
                "update": True,
                "query": False,
                "default": 0,
            },
            "status": {
                "create": 1,
                "read": True,
                "update": True,
                "query": False,
                "default": 1,
            },
            "last_sync_time": {
                "create": 1,
                "read": True,
                "update": True,
                "query": False,
                "default": "now()",
            },
            "updated_time": {
                "create": 1,
                "read": True,
                "update": False,
                "query": False,
                "default": "now()",
            },
        },
        conn=pool.connection(),
        verbose=2,
    )
    
    # r = c.create(
    #     pool.connection(), {"area_code": "k0000", "area_name": "kingdom", "level": 1}
    # )
    # r = c.page(
    #     pool.connection(), {"area_code": None, "area_name": "kingdom", "level": 1}
    # )
    r = c.update(
         {'id':11,"area_code": None, "area_name": "kingdom", "level": 1}
    )
    print(r)
