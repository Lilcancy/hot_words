## 环境

编程环境为python虚拟环境，使用sparkSQL完成开发。

spark版本：3.3.3

python版本：3.8.19

pyspark版本：3.2.0

## 在pycharm中运行

需要配置远程解释器，连接到linux的python虚拟环境中。

## 提交到spark上运行：

1) 删除`.setMaster("local[*]")`
2) 复制代码，在linux上创建一个`.py`文件并粘贴代码
3) 执行命令`/export/servers/spark/bin/spark-submit --master local[*] /root/main.py`，运行模式为local。

