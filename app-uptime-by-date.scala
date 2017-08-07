//计算指定app每日平均时长
val raw = spark.read.option("header","true").csv("hdfs://192.168.130.61/user/test/tmp/ap/a_2017")
val filtered = raw.filter($"package_name" === "com.tencent.mm")
val grouped = filtered.groupBy("date").agg(countDistinct("device_id"), sum("uptime"))
val result = grouped.orderBy(grouped("date"))
val result2 = result.select($"date", $"count(DISTINCT device_id)", $"sum(uptime)"/1000/60/60)
result2.show(999)

