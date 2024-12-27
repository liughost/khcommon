import mysql.connector


def mysql_connect(db_connect_info):
    """
    建立数据库连接
    "host": "172.16.1.61",
    "port": 3306,
    "user": "root",
    "password": "xxxxx",
    "database": "aaaaa",
    "auth_plugin": "mysql_native_password",
    "charset": "utf8mb4",
    """
    db_connect_info["auth_plugin"] = "mysql_native_password"
    db_connect_info["charset"] = "utf8mb4"
    return mysql.connector.connect(**db_connect_info)


def exec_select(conn, sql):
    """
    执行select语句
    """
    mycursor = conn.cursor()
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    fields = mycursor.description
    mycursor.close()
    # 转为字典的形式
    results = []
    for row in rows:
        d = {}
        n = 0
        for col in fields:
            d[col[0]] = row[n]
            n += 1
        results.append(d)
    conn.commit()
    return results


def exec_write_sql(conn, sql):
    """
    执行写入的SQL
    """
    mycursor = conn.cursor()
    mycursor.execute(sql)
    r = mycursor.rowcount
    mycursor.close()
    conn.commit()
    return r
