package com.geely.main.RES

import com.geely.general.{dateFunc, myUDF, schemas}
import mapreduce.SparkerTableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RESVehicle {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 初始化SparkSession
    val spark: SparkSession = SparkSession.builder.appName("resistance feature").enableHiveSupport.getOrCreate
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", 1000)

    // UDF 定义过滤规则myUDF.scala
    val arrayMedian = udf((cellVol: Seq[Double]) => myUDF.arrayMedian(cellVol))
    val voltageAllSame = udf((cellVol: Seq[Double]) => myUDF.voltageAllSame(cellVol))
    val tempAllSame = udf((cellTemp: Seq[Double]) => myUDF.tempAllSame(cellTemp))
    val tempNormal = udf((cellTemp: Seq[Double]) => myUDF.tempNormal(cellTemp))
    val interRes = udf((pbpv: Seq[Double], pbpvLead: Seq[Double], currentDiffer: Double) =>
      myUDF.internalResistance(pbpv: Seq[Double], pbpvLead: Seq[Double], currentDiffer: Double))
    val isZero = udf((interR: Seq[Double]) => myUDF.isZero(interR: Seq[Double]))
    val differ_iR = udf((iR_list: Seq[Double], iR_median: Double) => myUDF.differ_iR(
      iR_list: Seq[Double], iR_median: Double))
    val iRmedianRange = udf((iR_median: Double, declare_type: String) => myUDF.iRmedian_range(iR_median: Double, declare_type: String))


    // 外部输入参数
    val cell = args(0)
    val startTime = args(1) + " " + args(2)
    val stopTime = args(3) + " " + args(4)
    val outputPathRES = args(5)
    val vinList = args(6).split(",")


    // HBase配置
    val table = "iov:new_energy_vehicle_data" // HBASE表名
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    val config = HBaseConfiguration.create()
    config.set(SparkerTableInputFormat.INPUT_TABLE, table)
    config.set("hadoop.security.authentication", "Kerberos")
    config.set("hbase.master.kerberos.principal", "hbase/_HOST@GDMP.COM")
    config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@GDMP.COM")
    config.set("hbase.zookeeper.quorum", "10.86.210.26")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.client.scanner.timeout.period", "1200000")
    config.set("hbase.rpc.timeout", "1200000")
    config.set("zookeeper.znode.parent", "/hbase-secure")
    config.set("hbase.security.authentication", "kerberos")
    config.set("hadoop.security.authorization", "true")
    config.set("principal", "yanjiuy_npdk@GDMP.COM")
    config.set("keytab", "/home/yanjiuy_sesoft/yanjiuy_npdk.keytab")

    // SCAN查表
    // 1. 建立储存数据的空dataframe
    val schema = StructType(Seq(
      StructField("rowkey", StringType, nullable = true),
      StructField("json", StringType, nullable = true)
    ))
    var origin = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    var reverseVin: String = null
    var startRow: String = null
    var stopRow: String = null
    var proto: ClientProtos.Scan = null
    var scanToString: String = null

    // 2. 循环查表并存入上述dataframe
    for (vin <- vinList) {
      // 反转车架号，和时间拼接成rowkey
      reverseVin = vin.toUpperCase.reverse
      startRow = reverseVin + Integer.toString((dateFunc.date2TS(startTime) / 1000).toInt, 36).toUpperCase
      stopRow = reverseVin + Integer.toString((dateFunc.date2TS(stopTime) / 1000).toInt, 36).toUpperCase


      // 设置SCAN，并编码为BASE64
      val scan = new Scan
      scan.setStartRow(startRow.getBytes)
      scan.setStopRow(stopRow.getBytes)
      scan.addColumn("orgn".getBytes, "json".getBytes)
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(table))
      proto = ProtobufUtil.toScan(scan)
      scanToString = Base64.encodeBytes(proto.toByteArray)
      config.set(SparkerTableInputFormat.SCAN, scanToString)

      // 读取HBASE数据到RDD
      val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
        config,
        classOf[SparkerTableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      ).map(r => {
        val result = r._2
        val rowKey = Bytes.toString(result.getRow)
        val json = Bytes.toString(result.getValue("orgn".getBytes, "json".getBytes))
        Row(rowKey, json)
      })

      // 转换为dataframe
      val hbaseData = spark.createDataFrame(hbaseRDD, schema)
      origin = origin.union(hbaseData)
    }


    // 数据预处理
    val processed = origin.select(from_json(col("json"), schemas.featureStruct).as("json"))
      .select("json.*")
      .withColumn("timeStamp", unix_timestamp(col("sendingTime"), "yyyy-MM-dd HH:mm:ss").cast("long"))
      .withColumn("cells", explode(col("pbpv_data.cells")))
      .withColumn("pbpv_data", explode(col("pbpv_data.units")))
      .withColumn("temp_probes", explode(col("pbpt_data.totalPBProbes")))
      .withColumn("temp_probes", when(col("temp_probes").isNull, col("totalPBProbes")).otherwise(col("temp_probes")))
      .withColumn("pbpt_data", explode(col("pbpt_data.units")))
      .withColumn("mileage", col("mileage") / 10)
      .withColumn("total_current", col("total_current") / 10 - 1000)
      .withColumn("total_voltage", col("total_voltage") / 10)
      .withColumn("driveMotorData", col("driveMotorData.dataList.drivingMotorVoltage")(0))
      .withColumn("date", split(col("sendingTime"), " ")(0))


    // 数据筛选：
    // 1.	总电压total_voltage != 0；
    // 2.	单体电压数组pbpv_data长度 == 电池包电芯数cells；
    // 3.	温度探针数组pbpt_data长度 == 温度探针数temp_probes；
    // 4.	单体电压数组中所有单体电压不得全部相同；
    // 5.	温度探针数组中所有温度处于(-40, 85)℃且第2个探针开始往后不得全部相同；
    // 6.	去除绝缘阻值bir、单体电压pbpv_data、总电流total_current、驱动电机电压driveMotorData均一直发相同数值的数据（目的是去除BMS已经休眠，T-BOX未休眠仍在发的重复数据）。
    val filtered = processed.where(col("total_voltage") =!= 0) //总电压
      .where(size(col("pbpv_data")) === col("cells"))
      .where(size(col("pbpt_data")) === col("temp_probes"))
      .where(voltageAllSame(col("pbpv_data")) === true) //单体电压是否全部相等
      .where(tempAllSame(col("pbpt_data")) === true) //温度从第2个探针后不相同
      .where(tempNormal(col("pbpt_data")) === true) //温度（-40，85）
      .dropDuplicates("Vin", "bir", "total_current", "pbpv_data", "driveMotorData") //BMS休眠，T-BOX未休眠
      .drop("driveMotorData", "bir")


    // 获得车架号对应的PACK型号和OCV，计算单体SOC
    // 读入车架号-电池包型号对应关系表并持久化
    val vinPack = spark.sql(
      "SELECT vin AS V, " +
        "pkg_type AS P,  " +
        "declare_type " +
        "FROM viree_forward_search_dev.fs_dim_vehicle_info_df " +
        "WHERE pt IN (SELECT max(pt) FROM viree_forward_search_dev.fs_dim_vehicle_info_df)")

    // 获得车架号对应的PACK型号，筛选电流差
    val window: WindowSpec = Window.partitionBy("Vin", "date").orderBy("rowkey")
    val staticResWithInfo = filtered.join(vinPack, filtered("Vin") === vinPack("V"), "left")
      .where(col("P").isNotNull)
      .withColumn("pbpv_data_lead", lead("pbpv_data", 1).over(window)) //下一帧单体电压
      .withColumn("total_current_lead", lead("total_current", 1).over(window)) //下一帧总电流
      .withColumn("total_current_differ", abs(col("total_current_lead") - col("total_current"))) //电流差
      .drop("total_current_lead")
      .withColumn("Cap", regexp_extract(col("p"), "_(\\d+)Ah", 1).cast("Double").divide(10))
      .where(col("total_current_differ") >= col("Cap")) //前后相邻两帧总电流差>=0.1C


    //时间差小于等于11秒的数据帧，视作连续的数据帧
    //每辆车每日保留帧数大于等于100帧的数据
    val staticData = staticResWithInfo.withColumn("gap", lead("timeStamp", 1).over(window) - col("timeStamp"))
      .withColumn("gap", (col("gap") >= 11).cast("int"))
      .withColumn("gap", lag("gap", 1).over(window))
      .na.fill(0)
      .withColumn("group_id", sum("gap").over(window))


    val joinDF = staticData.groupBy("Vin", "date").agg(max("group_id").alias("num"))
      .where(col("num") >= 100)
      .withColumnRenamed("Vin", "v")
      .withColumnRenamed("date", "d")


    val joinExpr = (staticData("Vin") === joinDF("v")) && (staticData("date") === joinDF("d"))
    val staticRes = staticData.join(joinDF, joinExpr, "inner")
      .where(col("gap") === 0)
      .sort("rowkey")
      .drop("v", "d", "id")

    //算单体内阻值、中位内阻值：
    //1）	计算△U=|Uk-Uk+1|，保留△U均大于0或△U均小于0的帧；
    //2）	计算△I=| IK-IK+1 |；
    //3）	计算单体内阻值Ri=△U/△I，i=1，2，3……
    // 计算每帧数据的中位数内阻值medRes
    val originR = staticRes.withColumn("internal_Resistance", interRes(col("pbpv_data"),
      col("pbpv_data_lead"), col("total_current_differ")))
      .where(col("internal_Resistance").getItem(0).isNotNull)
      .withColumn("zeroFlag", isZero(col("internal_Resistance")))
      .where(col("zeroFlag").equalTo(true))
      .withColumn("iRmedian", arrayMedian(sort_array(col("internal_Resistance"))))
      .where(iRmedianRange(col("iRmedian"), col("declare_type")) === true)
      .withColumn("internal_Resistance_differ", differ_iR(col("internal_Resistance"), col("iRmedian")))
    //      .withColumn("internal_Resistance_per", col("internal_Resistance_differ")/col("iRmedian")*100)


    val resOUT = originR.select(col("Vin") +:
      col("sendingTime") +:
      col("date") +:
      col("timeStamp") +:
      col("mileage") +:
      col("rowkey") +:
      col("iRmedian") +:
      col("zeroFlag") +:
      (0 until cell.toInt).map(i => (col("internal_Resistance_differ")(i)).alias(s"$i")): _*
    ).groupBy("Vin", "date").agg(
      first("mileage").alias("startMileage"),
      last("mileage").alias("endMileage") +:
        count("rowkey").alias("num") +:
        (0 until cell.toInt).map(i => callUDF("percentile", col(s"$i"), lit(0.5)).alias(s"U${i + 1}")): _*
    )

    resOUT.repartition(1).write.mode("append").format("csv").option("header", "true").save(outputPathRES)

    spark.close()
  }


}
