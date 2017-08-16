import org.apache.spark.sql.types._

//每月活跃用户数最多的100个app
val month = "201706"
val ignored = List(
	"com.android.settings","com.miui.home","cn.apptimer.client","com.android.quicksearchbox",
	"com.android.systemui","com.miui.antispam","com.android.systemui","com.android.packageinstaller",
	"android","com.lbe.security.miui","com.google.android.packageinstaller","com.android.providers.calendar",
	"com.android.server.telecom","com.android.updater","com.android.providers.downloads","cn.apptimer.mrt.client",
	"com.miui.securityadd","com.android.incallui","com.bbk.launcher2","com.android.vpndialogs",
	"com.meizu.flyme.launcher","com.android.htmlviewer","com.android.contacts","com.android.providers.downloads.ui",
	"cn.apptimer.daily.client","com.android.deskclock","com.miui.securitycenter","com.android.camera",
	"com.android.calendar","com.android.mms","com.android.contacts","com.android.phone",
	"com.oppo.launcher","com.miui.systemAdSolution","com.android.dialer","com.xiaomi.gamecenter.sdk.service",
	"com.miui.powerkeeper","com.miui.voiceassist","com.coloros.recents","com.huawei.systemmanager",
	"com.huawei.android.launcher","com.sec.android.app.launcher"
)

val df0 = spark.read.option("header","true").csv("hdfs://192.168.130.60/user/test/tmp/ap/a_2017")
val df1 = df0.withColumn("package_name", trim($"package_name"))
val df2 = df1.filter($"hour" === -1).filter(substring($"date",0,6) === month).filter(not($"package_name".isin(ignored:_*)))
val df3 = df2.groupBy("package_name","name").agg(countDistinct("device_id"))
val df4 = df3.orderBy(desc(df3.columns(2)))

//distinct devices of the month
val devices = df2.agg(countDistinct("device_id")).first.getLong(0)

//add pct column
val df5 = df4.withColumn("pct", col(df4.columns(2)).cast(IntegerType)/devices)

df5.cache()
df5.show(100, false)

