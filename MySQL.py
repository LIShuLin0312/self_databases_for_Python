# !/usr/bin/env python3
# -*- coding: utf-8 -*-
from pymysql import connect
from pymysql.cursors import DictCursor
from zalo_pay_django.settings import DATABASES

dbs = DATABASES['default']['NAME']  # 数据库
user = DATABASES['default']['USER']
password = DATABASES['default']['PASSWORD']
host = DATABASES['default']['HOST']  # 端口


class MysqlDatabaseConnect():
    def __init__(self, database=dbs,
                 host=host,
                 user=user,
                 password=password,
                 charset='utf8mb4',
                 port=3306,
                 conn_close=True,
                 ):
        # 链接关闭信号默认每次关闭
        self.conn_close = conn_close
        # 创建链接
        self.conn = connect(database=database,  # 链接
                            host=host,
                            user=user,
                            password=password,
                            charset=charset,
                            port=port, )
        self.cur = self.conn.cursor()  # 游标

    def close(self):  # 关闭
        self.cur.close()
        self.conn.close()

    def __del__(self):  # 关闭
        if not self.conn_close:
            self.close()

    def workon(self, sql, val=None):  # 执行
        try:
            self.cur.executemany(sql, val) if val else self.cur.execute(sql)
            self.conn.commit()
            if self.conn_close:
                self.close()
            return True
        except BaseException as e:
            self.conn.rollback()
            print('数据库执行失败', e)
            print(sql)
            if self.conn_close:
                self.close()
            return False

    def workon_n(self, sql, val):  # 批量执行
        """
        sql = "INSERT INTO 表名(字段1,字段2,字段N,) VALUES (%s,%s,%s)"
        区别与单条插入数据，VALUES (%s,%s,%s) 里面不用引号
        val = (
            ('值1', '值2', '值N',),
            ('值1', '值2', '值N',),
             .....
            )
        """
        return self.workon(sql, val)

    def getAll(self, sql, n=False, dic=False):  # 查询结果多条     元祖((,),)
        """
        sql: 查询语句
        n:   查询条数False == all True == 1
        dic: 查询结果格式False == tuple  True == dict

        """
        if dic:
            self.cur.close()
            self.cur = self.conn.cursor(cursor=DictCursor)
        try:
            print(sql)
            self.cur.execute(sql)
            result = self.cur.fetchone() if n else self.cur.fetchall()
            if self.conn_close:
                self.close()
            return result
        except BaseException as e:
            self.conn.rollback()
            print('数据库查询失败', e, '\n', sql)
            if self.conn_close:
                self.close()
            return False

    def getAll_1(self, sql):  # 查询结果一条     元祖(,)
        return self.getAll(sql=sql, n=True)

    def getAll_dic(self, sql):  # 查询结果多条    字典[{},{},{},{}]
        return self.getAll(sql=sql, dic=True)

    def getAll_dic_1(self, sql):  # 查询结果一条    字典{:}
        return self.getAll(sql=sql, n=True, dic=True)

    def select(self, field, table_name, condition=None, count=None, n=False, dic=False):
        '''
        field: 要查询的字段[]  table_name: 表名''  condition: 条件{}  count:条数[开始,结束]
        '''
        Field = ','.join(field)
        condition = self.condition_dispose(condition)
        sql = '''select %s from %s%s;''' % (Field, table_name, condition)
        sql = sql + 'limt ' + ','.join(count) if count else sql + ';'
        return self.getAll(sql=sql, n=n, dic=dic)

    def select_1(self, *args):
        return self.select(*args, n=True)

    def select_dic(self, *args):
        return self.select(*args, dic=True)

    def select_dic_1(self, *args):
        return self.select(*args, n=True, dic=True)

    def insert(self, field, table_name):
        '''
        field: 要插入的 {字段:值}   table_name:表名 ''
        '''
        Field, Price = '', ''
        if field:
            for key, vul in field.items():
                Field += key + ','
                Price += str(vul) + ',' if type(vul) == int else '"' + str(vul) + '",'

        sql = 'insert into %s(%s) values(%s);' % (table_name, Field[:-1], Price[:-1])
        return self.workon(sql)

    def insert_n(self, field, table_name, val, skip=False):
        '''
        field: 要插入的 [字段]   table_name:表名 ''   val: 二维元祖    skip: 是否跳过重复插入
        '''
        sql = 'insert into %s(%s) values(%s)' % (table_name, ','.join(field), '%s,' * len(field)[:-1],)
        sql = sql + ' ON duplicate KEY UPDATE id=id;' if skip else sql
        return self.workon(sql, val)

    def insert_n_skip(self, *args):
        return self.insert_n(*args, skip=True)

    def updata(self, field, table_name, condition=None):
        '''
        field: 要修改的字段{}   table_name: 表名''     condition: 条件{}
        '''
        field_L = []
        if field:
            for key, vul in field.items():
                field_L.append(key + '=' + (str(vul) if type(vul) == int else '"' + str(vul) + '"'))
            field = ','.join(field_L)
        condition = self.condition_dispose(condition)
        sql = '''UPDATE %s SET %s%s;''' % (table_name, field, condition)
        return self.workon(sql)

    def delete(self, table_name, condition=None):
        '''
         table_name: 表名''     condition: 条件{}
        '''
        condition = self.condition_dispose(condition)
        sql = '''delete from %s%s;''' % (table_name, condition)
        # print(sql)
        return self.workon(sql)

    def condition_dispose(self, condition):  # where 条件拼接
        Condition = []
        if condition:
            for key, vul in condition.items():
                Condition.append(key + '=' + (str(vul) if type(vul) == int else '"' + str(vul) + '"'))
            condition = ' where ' + ' and '.join(Condition)
        else:
            condition = ''
        return condition


if __name__ == '__main__':
    table_name = 'xxxxx'
    field = None
    condition = None
    sql = '''UPDATE %s SET %s%s;''' % (table_name, field, condition)
    print(sql)
    # Mysqlcz().workon(sql)
    Mysqlcz().delete('cs', {'xxx': 'eee'})
    # a= Mysqlcz(database='vmail').updata({'count': 20}, 'mailbox', {'username': 'ahouahouwei184059@mail.ultratechcom.com'})
    # if a:
    #     print('成功')
    # else:
    #     print('失败')
    a = {}

#     Mysqlcz().workon('''
#     insert into python_momo_MQ_push_history(serial_number,device_number,momo_accout,shift_to_phone,sum,time,push_status) values("102","QFJ9X18413G
# 01358","0866470350","01297690342","10000","2019-09-11 14:43:41",1);
#
#     ''')
