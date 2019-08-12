import pymongo


class Mongo_db():
    def __init__(self):
        # 创建连接对象
        self.client = pymongo.MongoClient(host='localhost', port=27017)
        # 指定数据库
        self.db = self.client.mfw_city
        # 指定操作的集合
        self.collection = self.db.city_gaishu

    # 插入单条数据，返回生成的ID
    def insert_data(self, data):
        try:
            self.collection.insert_one(data)
            print("插入执行成功")
            return self.collection.inserted_id
        except Exception as f:
            print("插入执行失败，错误原因：", f)

    # 插入多条数据，需要列表传入,返回生成的ID列表
    def insert_datas(self, datas):
        try:
            self.collection.insert_many(datas)
            print("插入执行成功")
            return self.collection.inserted_ids
        except Exception as f:
            print("插入执行失败，错误原因：", f)

    # 查询单条数据，返回查询结果
    def get_data(self, keydata, valuedata):
        data = self.collection.find_one({keydata: valuedata})
        return data

    # 查询全部数据，返回查询结果的生成器对象
    def get_datas(self, keydata, valuedata):
        datas = self.collection.find({keydata: valuedata})
        return datas

    # 统计多头查询的数量
    def get_datas_len(self, keydata, valuedata):
        data = self.collection.find({keydata: valuedata}).count()
        return data

    # 更新数据。keydata：指定的查询/更新建，valuedata：指定的查询指，updatevalue：指定更新的值
    def update_data(self, keydata, valuedata, updatevalue):
        try:
            condition = {keydata: valuedata}
            student = self.collection.find_one(condition)
            student[keydata] = updatevalue
            result = self.collection.update(condition, student)
            print("更新执行成功")
        except Exception as f:
            print("更新执行失败，错误原因：", f)

    # 删除数据
    def removes(self,keydata, valuedata):
        try:
            result = self.collection.remove({keydata: valuedata})
            print("删除执行成功")
        except Exception as f:
            print("删除执行失败，错误原因：", f)