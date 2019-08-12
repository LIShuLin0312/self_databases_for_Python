from pymysql import connect

dbs = 'mfw'  # 数据库
host="127.0.0.1" # 端口
user='root'
password='password'


class Mysqlcz:
    def __init__(self, database=dbs,
                 host=host,
                 user=user,
                 password=password,
                 charset='utf8',
                 port=3306):
        # 创建链接
        self.conn = connect(database=database,  # 链接
                            host=host,
                            user=user,
                            password=password,
                            charset=charset,
                            port=port)
        self.cur = self.conn.cursor()  # 游标
# 关闭

    def close(self):
        self.cur.close()
        self.conn.close()
# 执行

    def workon(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
            print('数据库插入执行成功')
        except Exception as e:
            self.conn.rollback()
            print('数据库插入执行失败', e)
            print(sql)
        self.close()
# 查询

    def getAll(self, sql):
        try:
            self.cur.execute(sql)
            # print('数据库查询执行成功')
            result = self.cur.fetchall()
            self.close()
            return result
        except Exception as e:
            self.conn.rollback()
            print('数据库查询执行失败', e)
        self.close()
