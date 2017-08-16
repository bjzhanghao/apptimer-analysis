//val APP = "com.ss.android.article.news"
//val APP = "com.tencent.mobileqq"
//val APP = "com.sina.weibo"
//val APP = "com.tencent.mm"
//val APP = "com.taobao.taobao"
//val APP = "com.zhihu.android"
//val APP = "com.eg.android.AlipayGphone"
//val APP = "com.netease.cloudmusic"
//val APP = "com.jingdong.app.mall"
//val APP = "com.autonavi.minimap"
//val APP = "com.baidu.BaiduMap"
//val APP = "com.tmall.wireless"
//val APP = "com.evergrande.eif.android.hengjiaosuo" //too few data
//val APP = "com.bank9f.weilicai" //too few data
//val APP = "com.ppdai.loan"  //too few data
//val APP = "com.ucredit.financial.android" //人人贷
//val APP = "so.ofo.labofo"
//val APP = "com.mobike.mobikeapp"
//val APP = "com.beastbike.bluegogo" //小蓝单车
//val APP = "me.ele"
//val APP = "com.baidu.lbs.waimai"
//val APP = "com.sankuai.meituan.takeoutnew"
//val APP = "com.tencent.map"
//val APP = "com.suning.mobile.ebuy"
//val APP = "com.dangdang.buy2"
val APP = "tv.danmaku.bili"

//读取原始数据并做必要过滤
val raw = spark.read.option("header","true").csv("hdfs://192.168.130.60/user/test/tmp/ap/a_2017")
val df0 = raw.filter($"hour" === -1).filter(length($"date") === 8)

//计算指定app每日活跃设备数和使用时长
val df2 = df0.filter($"package_name" === APP)
val df4 = df2.groupBy("date").agg(countDistinct("device_id").as("app_devices"), sum("uptime").as("app_uptime"))
val df8 = df4.select($"date", $"app_devices", $"app_uptime")

//计算每日总活跃设备数和总使用时长
val df10 = df0.groupBy("date").agg(countDistinct("device_id").as("all_devices"), sum("uptime").as("all_uptime"))
val df12 = df10.select($"date", $"all_devices", $"all_uptime")

//根据date组合两份数据集
val df20 = df8.join(df12, "date")

//计算app占比
val df30 = df20.withColumn("deviceRatio", $"app_devices"/$"all_devices" )
val df32 = df30.withColumn("uptimeRatio", $"app_uptime"/$"all_uptime" )

//调整输出格式
val df34 = df32.orderBy("date")
val df36 = df34.withColumn("date", concat(substring($"date",1,4), 
	lit("-"), substring($"date",5,2), lit("-"), substring($"date",7,2)))
val df38 = df36.withColumn("app_uptime", $"app_uptime"/1000)
val df40 = df38.withColumn("all_uptime", $"all_uptime"/1000)
val df42 = df40.withColumn("deviceRatio", format_number($"deviceRatio",6))
val df44 = df42.withColumn("uptimeRatio", format_number($"uptimeRatio",6))

df44.show(999)

