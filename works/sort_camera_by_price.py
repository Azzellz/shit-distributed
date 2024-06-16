from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建SparkSession对象
spark = SparkSession.builder.appName("camera").getOrCreate()

# 加载CSV文件到DataFrame
df = spark.read.format("csv").option("header", "true").load("/camera.csv")

# 转换价格列为数字类型
df = df.withColumn("Price", df["Price"].cast("double"))

# 按年份和型号分组，然后计算平均价格
result = df.groupBy("Release date", "Model").agg(F.avg("Price").alias("Average Price"))

# 按平均价格排序
result = result.sort("Average Price", ascending=False)

# 显示结果
result.show()