//计算指定app每日平均时长
val app = "com.ss.android.article.news"
val raw = spark.read.option("header","true").csv("hdfs://192.168.130.60/user/test/tmp/ap/a_2017")
val df2 = raw.filter($"package_name" === month)
val df4 = df2.groupBy("date").agg(countDistinct("device_id"), sum("uptime"))
val df6 = df4.orderBy(df4("date"))
val df8 = df6.select($"date", $"count(DISTINCT device_id)", $"sum(uptime)"/1000/60/60)
df8.show(999)

