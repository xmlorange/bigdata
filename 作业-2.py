
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 创建SparkSession
spark = SparkSession.builder.config("spark.default.parallelism", "1000").config("spark.driver.memory", "2G").config("spark.driver.cores", "2").config("spark.executor.memory", "2G").config("spark.executor.cores", "2").enableHiveSupport().getOrCreate()

# 定义数据模式
schema = StructType([
    StructField("班级", IntegerType(), True),
    StructField("姓名", StringType(), True),
    StructField("年龄", IntegerType(), True),
    StructField("性别", StringType(), True),
    StructField("科目", StringType(), True),
    StructField("成绩", IntegerType(), True)
])

# 读取数据
df = spark.read.csv("test.txt", sep=" ", schema=schema)

# 一共有多少个小于20岁的人参加考试
less_than_20 = df.filter(df["年龄"] < 20).count()
less_than_20.show()

# 一共有多少个等于20岁的人参加考试
equal_to_20 = df.filter(df["年龄"] == 20).count()
equal_to_20.show()

# 一共有多少个大于20岁的人参加考试
greater_than_20 = df.filter(df["年龄"] > 20).count()
greater_than_20.show()

# 一共有多少个男生参加考试
male_count = df.filter(df["性别"] == "男").count()
male_count.show()

# 一共有多少个女生参加考试
female_count = df.filter(df["性别"] == "女").count()
female_count.show()

# 12班有多少人参加考试
class_12_count = df.filter(df["班级"] == 12).count()
class_12_count.show()

# 13班有多少人参加考试
class_13_count = df.filter(df["班级"] == 13).count()
class_13_count.show()

# 语文科目的平均成绩
chinese_avg = df.filter(df["科目"] == "chinese").agg(avg("成绩")).collect()[0][0]
chinese_avg.show()

# 数学科目的平均成绩
math_avg = df.filter(df["科目"] == "math").agg(avg("成绩")).collect()[0][0]
math_avg.show()

# 英语科目的平均成绩
english_avg = df.filter(df["科目"] == "english").agg(avg("成绩")).collect()[0][0]
english_avg.show()

# 每个人平均成绩
individual_avg = df.groupBy("班级", "姓名", "年龄", "性别").agg(avg("成绩").alias("平均成绩"))
individual_avg.show()

# 12班平均成绩
class_12_avg = df.filter(df["班级"] == 12).agg(avg("成绩")).collect()[0][0]
class_12_avg.show()

# 12班男生平均总成绩
class_12_male_avg = df.filter((df["班级"] == 12) & (df["性别"] == "男")).groupBy("班级", "姓名", "年龄", "性别").agg(avg("成绩").alias("平均成绩")).agg(avg("平均成绩")).collect()[0][0]
class_12_male_avg.show()

# 12班女生平均总成绩
class_12_female_avg = df.filter((df["班级"] == 12) & (df["性别"] == "女")).groupBy("班级", "姓名", "年龄", "性别").agg(avg("成绩").alias("平均成绩")).agg(avg("平均成绩")).collect()[0][0]
class_12_female_avg.show()

# 13班平均成绩
class_13_avg = df.filter(df["班级"] == 13).agg(avg("成绩")).collect()[0][0]
class_13_avg.show()

# 13班男生平均总成绩
class_13_male_avg = df.filter((df["班级"] == 13) & (df["性别"] == "男")).agg(avg("成绩")).collect()[0][0]
class_13_male_avg.show()

# 13班女生平均总成绩
class_13_female_avg = df.filter((df["班级"] == 13) & (df["性别"] == "女")).agg(avg("成绩")).collect()[0][0]
class_13_female_avg.show()

# 全校语文成绩最高分
chinese_max = df.filter(df["科目"] == "chinese").agg(max("成绩")).collect()[0][0]
chinese_max.show()

# 12班语文成绩最低分
class_12_chinese_min = df.filter((df["班级"] == 12) & (df["科目"] == "chinese")).agg(min("成绩")).collect()[0][0]
class_12_chinese_min.show()


spark.stop()

