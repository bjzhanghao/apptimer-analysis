//val APP = "com.ss.android.article.news" //今日头条
//val APP = "com.ss.android.article.lite" //今日头条极速版
//val APP = "com.tencent.reading" //天天快报
//val APP = "com.tencent.readingplus" //天天快报大字版 (few)
//val APP = "com.tencent.mobileqq"
//val APP = "com.sina.weibo"
//val APP = "com.tencent.mm"
//val APP = "com.taobao.taobao"
//val APP = "com.immomo.momo"
//val APP = "com.duowan.mobile" //YY
//val APP = "com.ss.android.essay.joke" //内涵段子
//val APP = "com.kuaikan.comic" //快看漫画
//val APP = "com.zhihu.android"
//val APP = "com.eg.android.AlipayGphone"
//val APP = "com.netease.cloudmusic"
//val APP = "com.jingdong.app.mall"
//val APP = "com.autonavi.minimap"
//val APP = "com.baidu.BaiduMap"
//val APP = "com.youku.phone"
//val APP = "com.youku.tv" //优酷大屏版 (few)
//val APP = "com.tencent.qqlive" //腾讯视频
//val APP = "com.tencent.qqlivehd" //腾讯视频大屏版 (few)
//val APP = "com.qiyi.video"
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
//val APP = "com.smile.gifmaker" //快手
//val APP = "com.ss.android.ugc.aweme" //抖音
//val APP = "com.ss.android.article.video" //西瓜视频
val APP = "com.lemon.faceu" //faceu激萌
//val APP = "tv.danmaku.bili"
//val APP = "com.mt.mtxx.mtxx" //美图秀秀
//val APP = "com.meitu.meiyancamera" //美颜相机
//val APP = "com.meitu.meipaimv" //美拍
//val APP = "com.meitu.wheecam" //潮自拍
//val APP = "com.sdu.didi.psnger"
//val APP = "com.sdu.didi.gsui" //滴滴司机端
//val APP = "com.mojang.minecraftpe" //我的世界
//val APP = "com.tencent.tmgp.sgame" //王者荣耀

//读取原始数据并做必要过滤
val raw17 = spark.read.option("header","true").csv("hdfs://192.168.130.60/user/test/tmp/ap/a_2017")
val raw16 = spark.read.option("header","true").csv("hdfs://192.168.130.60/user/test/tmp/ap/a_2016")
val raw = raw16.union(raw17)
val df0 = raw.filter($"hour" === -1).filter(length($"date") === 8).withColumn("month",substring($"date",1,6))

//计算指定app每月活跃设备数和使用时长
val df2 = df0.filter($"package_name" === APP)
val df4 = df2.groupBy("month").agg(countDistinct("device_id").as("app_devices"), sum("uptime").as("app_uptime"))
val df8 = df4.select($"month", $"app_devices", $"app_uptime")

//计算每月总活跃设备数和总使用时长
val df10 = df0.groupBy("month").agg(countDistinct("device_id").as("all_devices"), sum("uptime").as("all_uptime"))
val df12 = df10.select($"month", $"all_devices", $"all_uptime")

//根据date组合两份数据集
val df20 = df8.join(df12, "month")

//计算app占比
val df30 = df20.withColumn("deviceRatio", $"app_devices"/$"all_devices" )
val df32 = df30.withColumn("uptimeRatio", $"app_uptime"/$"all_uptime" )

//调整输出格式
val df34 = df32.orderBy("month")
val df36 = df34.withColumn("month", concat(substring($"month",1,4), 
	lit("-"), substring($"month",5,2)))
val df38 = df36.withColumn("app_uptime", $"app_uptime"/1000)
val df40 = df38.withColumn("all_uptime", $"all_uptime"/1000)
val df42 = df40.withColumn("deviceRatio", format_number($"deviceRatio",6))
val df44 = df42.withColumn("uptimeRatio", format_number($"uptimeRatio",6))

df44.show(999)

