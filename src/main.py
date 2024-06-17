# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.functions import  split, col, desc

"""
根据用户上网的搜索记录对每天的热点搜索词进行统计，以了解用户所关心的热点话题。
要求完成：统计每天搜索数量前3名的搜索词（同一天中同一用户多次搜索同一个搜索词视为1次）。
"""

if __name__ == '__main__':
    # 构建sparksession
    spark = SparkSession.builder\
        .appName('hot_words')\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", "2")\
        .getOrCreate()
    sc = spark.sparkContext

    # 读取txt文件
    df = spark.read.format("csv")\
        .option("sep", ",")\
        .option("header", False)\
        .option("encoding", "utf-8")\
        .schema("date STRING, name STRING, word STRING")\
        .load("hdfs://192.168.121.131:9000/input/keywords.txt")
    # df.show(truncate=False)

    # 数据去重
    df1 = df.dropDuplicates()
    # df1.show(truncate=False)

    # 对日期和搜索词GroupBy
    groupbyed_df1 = df1.groupBy("date", "word").count().orderBy(["date", "word"], ascending=[True,True])

    # TODO: 获取当天搜索数量前3的搜索词
    date_list = ["2019-10-01", "2019-10-02", "2019-10-03"]
    broadcast = sc.broadcast(date_list)
    values = broadcast.value

    acmlt = sc.accumulator(0)
    for value in values:
        result = groupbyed_df1.where(groupbyed_df1["date"] == value).limit(3)
        result.show(truncate=False)
        # 写出到hdfs
        result.write.mode("overwrite").format("csv")\
            .option("seq", ",")\
            .save(f"hdfs://192.168.121.131:9000/output/hot_words_output/output{acmlt}/")
        acmlt += 1
    print("ok")
    spark.stop()