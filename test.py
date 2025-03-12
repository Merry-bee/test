from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf

spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
# 读入数据并创建view
df = spark_session.read.csv('datacamp_ecommerce.csv',header=True,escape="\"")
# df.show(10)
rdd = df.rdd.repartition(2)
def func3(row):
    if float(row.UnitPrice) >= 5:
        return True
    else:
        return False
rdd7 = rdd.filter(func3)
# rdd7 = rdd.filter(lambda row: float(row.UnitPrice) >= 5)
print(rdd.count(),rdd7.count())
# df.createOrReplaceTempView("Ecommerce")
# # 使用HiveSQL对view进行查询
# sql = '''select * from Ecommerce limit 3'''
# df2 = spark_session.sql(sql)
# df2.show()
# # 使用mapPartition处理rdd
# rdd2 = df2.rdd.repartition(2)
# def func(partitionData):
#     res = []
#     for row in partitionData:
#         country = row.Country
#         price = int(row.Quantity)*float(row.UnitPrice)
#         res.append([country,price])
#         # res.append(row)
#     return iter(res)
# def func2(row):
#     country = row.Country
#     price = int(row.Quantity)*float(row.UnitPrice)
#     return [country,price]

# rdd3 = rdd2.mapPartitions(func)
# rdd4 = rdd2.map(func2)
# df3 = rdd3.toDF(['country','price'])
# df3.show()
# print('this is a line')
# df4 = rdd4.toDF(['country','price'])
# df4.show()
# print('this is a line')


# # 准备示例文本数据，创建一个RDD
# string_col = ["Hello world", "How are you", "Python is great"]
# num_col = [1,2,3,4,5]
# rdd5 = spark_session.sparkContext.parallelize(list(zip(string_col,num_col)))

# # 定义flatMap使用的函数，这里使用split函数将字符串按空格拆分成单词列表
# def split_words(row):
#     words_lst = row[0].split(' ')
#     num = row[1]
#     result = []
#     for word in words_lst:
#         result.append((word,num))
#     return result

# # 使用flatMap操作
# rdd6 = rdd5.flatMap(split_words)
# rdd6.toDF().show()

# # 打印结果
# rdd6.foreach(print)