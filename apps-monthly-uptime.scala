//import org.apache.spark.sql.functions._

//将用apps-mat.scala取代

//每月uptime最高的50个app
val df0 = spark.read.option("header","true").csv("hdfs://192.168.130.61/user/test/tmp/ap/a_2017")
val df1 = df0.filter($"hour" === -1).filter($"date".between("20170301","20170331"))
val df2 = df1.groupBy("package_name","name").agg(count("*"), sum("uptime"))
val df3 = df2.orderBy(desc("sum(uptime)"))
val df4 = df3.select($"package_name", $"name", $"sum(uptime)"/1000)
df4.cache()
df4.show(50, false)

