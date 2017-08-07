val df0 = spark.read.option("header","true").csv("hdfs://192.168.130.61/user/test/tmp/ap/a_2017")
val df1 = df0.filter(length($"date") === 8) //there are a few illegal records
val df2 = df1.groupBy(substring($"date",0,6)).agg(countDistinct("device_id"))
val df3 = df2.orderBy(asc(df2.columns(0)))
df3.cache()
df3.show(false)
