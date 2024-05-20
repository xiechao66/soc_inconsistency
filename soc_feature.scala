package com.geely.main.soc.v3

import com.geely.general.{dateFunc, myUDF}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SOCfeature {

  /**
   * 数据频率为10s
   * 整包欠压过压，每天输出
   *
   */

  def main(args: Array[String]): Unit = {

    // 配置
    val spark: SparkSession = SparkSession.builder.appName("soc feature manual hive").enableHiveSupport.getOrCreate
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", 1000)
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    // UDF
    val arrayMedian: UserDefinedFunction = udf((arr: Seq[Double]) => myUDF.arrayMedian(arr)) //中位数
    val arraySOC: UserDefinedFunction = udf((arr: Seq[Double], pack: String) => myUDF.arraySOC_NCM(arr, pack)) //反查SOC
    val voltageNormal: UserDefinedFunction = udf((cellVol: Seq[Double]) => myUDF.voltageNormal(cellVol)) //电压范围
    val voltageAllSame: UserDefinedFunction = udf((cellVol: Seq[Double]) => myUDF.voltageAllSame(cellVol)) //电压是否全部相同
    val tempAllSame: UserDefinedFunction = udf((cellTemp: Seq[Double]) => myUDF.tempAllSame(cellTemp))
    val tempNormal: UserDefinedFunction = udf((cellTemp: Seq[Double]) => myUDF.tempNormal(cellTemp))
    val int2Double: UserDefinedFunction = udf((cellVol: Seq[Int]) => myUDF.int2Double(cellVol)) //转换arrayType的类型
    val short2Double: UserDefinedFunction = udf((cellTemp: Seq[Short]) => myUDF.short2Double(cellTemp))
    val volJudge: UserDefinedFunction = udf((cellVol: Seq[Double]) => myUDF.judge_vol_ncm(cellVol)) //判断电压区间
    val socJudge: UserDefinedFunction = udf((cellsoc: Seq[Double]) => myUDF.judge_soc(cellsoc)) //判断SOC正负
    val diffSOC: UserDefinedFunction = udf((cellsoc: Seq[Double], SOC_median: Double) => myUDF.differ_SOC(cellsoc, SOC_median)) //判断SOC正负

    // 输入参数：soc表示仅输出soc特征，temp表示仅输出温度特征，为空则同时输出
    val pt: String = args(0)
    val pt2: String = dateFunc.chargeDateFormat(pt, "yyyyMMdd", "yyyy-MM-dd")
    val cell: String = args(1)
    val mincell: String = args(2)
    val minprobe: String = args(3)
    val outputPath = args(4)

    // 读取数据
    val origin: Dataset[Row] = spark.sql(
      "SELECT " +
        "vin, " +
        "sending_time AS sendingTime, " +
        "CAST(vehicle_bir AS FLOAT) AS bir, " +
        "CAST(vehicle_total_current AS FLOAT) AS total_current, " +
        "CAST(vehicle_total_voltage AS FLOAT) AS total_voltage, " +
        "temp_units AS pbpt_data, " +
        "total_pb_probes AS temp_probes, " +
        "extreme_max_vofcells AS max_vofcells, " +
        "extreme_min_vofcells AS min_vofcells, " +
        "vol_units AS pbpv_data, " +
        "batt_total_cells AS cells, " +
        "rowkey, " +
        "CAST(vehicle_mileage AS FLOAT) AS mileage, " +
        "CAST(vehicle_charging_status AS INT) AS charging_status, " +
        "dr_motor_voltage AS driveMotorData, " +
        "vehicle_soc AS SOC, " +
        "car_type, " +
        "pkg_type, " +
        "declare_type " +
        "FROM viree_forward_search_dev.fs_dwd_new_big_vehicle_full_data_di " +
        s"WHERE pt IN ($pt)" +
        "AND car_type IN (\'DC1E\', \'EF1E\', \'BX1E\', \'CS1E\', \'DC1E-FR\') " +
        "AND declare_type IN ('MR7001BEV03', 'MR7001BEV04', 'MR7001BEV05', 'MR7001BEV06', 'MR7001BEV08', 'MR7001BEV11', 'MR7001BEV15', 'MR7001BEV16', 'MR7001BEV17', 'MR7001BEV18', 'MR7001BEV20', 'MR7001BEV21', 'MR7001BEV22', " +
        " 'MR6525BEV04', 'MR6525BEV05', 'MR6525BEV06', " +
        " 'MR7005BEV05', 'MR7005BEV07', 'MR7005BEV09', " +
        " 'MR7005BEV15', 'MR7005BEV12', " +
        " 'MR7001BEV27')"
    ).where(col("sendingTime") >= (pt2 + " 00:00:00") && col("sendingTime") <= (pt2 + " 23:59:59"))

    // M241数据
    spark.sql(
      "SELECT " +
      "vin, portstarttimestamp, bkpofdsttrvld, ")


    // 数据预处理
    val processed: DataFrame = origin.withColumn("timeStamp", unix_timestamp(col("sendingTime"), "yyyy-MM-dd HH:mm:ss").cast("long"))
//      .withColumn("mileage", col("mileage") / 10)
//      .withColumn("total_current", col("total_current") / 10 - 1000)
//      .withColumn("total_voltage", col("total_voltage") / 10)
      .withColumn("date", split(col("sendingTime"), " ")(0))


    //-------------------------------------------------------SOC--------------------------------------------------------
    // 数据筛选：
    // 1.电池包总压不得为0
    // 2.最大单体电压小于4400mv，最小单体电压大于2500mv
    // 3.筛选出电流介于[-2, 2]A的伪静置数据
    // 4.去除绝缘阻值、单体电压、总电流、电机控制器输入电压、里程均一直发相同数值的帧（目的是去除BMS已经休眠，T-BOX未休眠仍在发的重复数据）
    // 5.去除单体电压数组长度不足95的数据
    // 6.去除单体电压数组中数值全部相同的数据
    // 7.去除单体电压数组中存在超出[2500, 4400]范围的数据
    val filtered: DataFrame = processed.where(col("charging_status") === 3)
      .where(col("total_voltage") =!= 0)
      .where(col("total_current") >= -2 && col("total_current") <= 2)
      .dropDuplicates("Vin", "bir", "total_current", "pbpv_data", "driveMotorData", "mileage")
      .where(col("cells") >= mincell.toInt)
      .where(col("temp_probes") >= minprobe.toInt)
      .where(size(col("pbpt_data")) === col("temp_probes"))
      .where(size(col("pbpv_data")) === col("cells"))
      .withColumn("pbpv_data", int2Double(col("pbpv_data")))
      .withColumn("pbpt_data", short2Double(col("pbpt_data")))
      .where(tempAllSame(col("pbpt_data")) === true) //温度从第2个探针后不相同
      .where(tempNormal(col("pbpt_data")) === true) //（-40，85）
      .where(col("max_vofcells") <= 4.950) //电压筛选
      .where(col("min_vofcells") >= 0.500)
      .drop("driveMotorData", "bir")
    filtered.cache()

    // 判断电压范围
    // 输出整包过压、欠压报警
    val vol_judge: DataFrame = filtered.withColumn("vol_flag", volJudge(col("pbpv_data")))

    val warnCars: Dataset[Row] = vol_judge.groupBy("vin", "date", "vol_flag").count()
      .withColumn("per_flag", col("count") / sum("count").over(
        Window.partitionBy("vin", "date")))
      .where(col("count")>=10 && col("per_flag")>=0.5 && col("vol_flag").isNotNull &&
        (col("vol_flag")===Array(1) || col("vol_flag")===Array(2)))
    //    warnCars.show()

    if (warnCars.count() > 0){
      // 正常处理逻辑

      val outWarn: DataFrame = vol_judge
        .join(broadcast(warnCars), Seq("vin", "date", "vol_flag"),"inner")
        .withColumn("endMileage", first("mileage").over(
          Window.partitionBy("vin", "date").orderBy(col("rowkey").desc)))
        .groupBy("vin", "date").agg(
        first("car_type").alias("car_type"),
        first("pkg_type").alias("pkg_type"),
        first("declare_type").alias("declare_type"),
        first("endMileage").alias("endMileage")
      ).join(warnCars, Seq("vin", "date"), "inner")

      //输出报警
      val resultWarn: DataFrame = outWarn
        .withColumn("warn_level", lit("3"))
        .withColumn("longitude", lit("None"))
        .withColumn("latitude", lit("None"))
        .withColumn("start_time", col("date"))
        .withColumn("end_time", col("date"))
        .withColumn("abnormal_units", lit("PACK"))
        .withColumn("suggestion", lit("更换模组，请立刻维修"))
        .withColumn("mileage", col("endMileage"))
        .withColumn("fault_details", when(col("vol_flag") === Array(1), lit("整包欠压"))
          .when(col("vol_flag") === Array(2), lit("整包过压")))
        .withColumn("warn_kind", lit("18"))
        .select("vin", "warn_level", "longitude", "latitude", "start_time", "end_time", "abnormal_units", "suggestion",
          "mileage", "fault_details", "warn_kind", "car_type", "pkg_type", "declare_type")

//      resultWarn.
//            repartition(1).write.mode("append").format("csv").option("header", "true").save(outputPath + "/warn")
      resultWarn.createTempView("warnOUT")
      spark.sql(
        s" INSERT INTO TABLE viree_forward_search_dev.fs_dws_soc_consistency_predict_wi_tmp " +
          s"PARTITION (pt=$pt) " +
          "SELECT * FROM warnOUT")

    } else {
      println("当天没有整包过压欠压报警")
    }


    // 筛选出连续30秒以上的伪静置片段
    val window: WindowSpec = Window.partitionBy("Vin", "date").orderBy("rowkey")
    val staticData: DataFrame = filtered
      .where(col("max_vofcells") <= 4.400) //电压筛选
      .where(col("min_vofcells") >= 2.500)
      .where(voltageAllSame(col("pbpv_data")) === true)
      .where(voltageNormal(col("pbpv_data")) === true)
      .withColumn("gap", col("timeStamp") - lag("timeStamp", 1).over(window))
      .withColumn("gap", (col("gap") >= 11).cast("int")) //>=11,gap=0
      .na.fill(0)
      .withColumn("group_id", sum("gap").over(window))

    val joinDF: DataFrame = staticData.groupBy("Vin", "date", "group_id")
      .agg(
        (max("timeStamp")-min("timeStamp")).alias("timediff")
      ).where(col("timediff") > 30)
      .withColumnRenamed("Vin", "v")
      .withColumnRenamed("date", "d")
      .withColumnRenamed("group_id", "id")

    val joinExpr: Column = staticData("Vin") === joinDF("v") && (staticData("date") === joinDF("d")) && (staticData("group_id") === joinDF("id"))
    val staticRes: DataFrame = staticData.join(joinDF, joinExpr, "left_semi")
      .sort("rowkey")
      .drop("v", "d", "id")

    // 获得车架号对应的PACK型号和OCV，计算单体SOC
    val staticResWithInfo: DataFrame = staticRes
      .withColumn("cellSOC", arraySOC(col("pbpv_data"), col("declare_type")))
      .withColumn("medSOC", arrayMedian(sort_array(col("cellSOC"))))
      .where(col("medSOC") <= 100)
      .drop("V", "pbpv_data", "P")

    val array_list: Array[String] = Array.tabulate(cell.toInt)(i => s"avg${i + 1}")

    val socOUT: DataFrame = staticResWithInfo.select(col("Vin") +:
      col("car_type") +:
      col("sendingTime") +:
      col("date") +:
      col("timeStamp") +:
      col("mileage") +:
      col("rowkey") +:
      col("medSOC") +:
      (0 until cell.toInt).map(i => (col("cellSOC")(i) - col("medSOC")).alias(s"$i")): _*
    ).groupBy("vin", "date", "car_type").agg(
      first("mileage").alias("startMileage"),
      last("mileage").alias("endMileage") +:
        count("rowkey").alias("num") +:
        (0 until cell.toInt).map(i => avg(abs(col(s"$i"))).alias(s"avg${i + 1}")): _*
    ).withColumn("delta_soc", array(array_list.map(col): _*))
      .select("vin", "date", "startMileage", "endMileage", "num", "delta_soc", "car_type")

//    socOUT.
//      repartition(1).write.mode("append").format("json").save(outputPath + "/soc_feature")
    socOUT.createTempView("socOUT")
    spark.sql(
      s" INSERT OVERWRITE TABLE viree_forward_search_dev.fs_dws_soc_feature_v3_di " +
        s"PARTITION (pt=$pt, car_type) " +
        "SELECT vin,date,startMileage,endMileage,num,delta_soc, car_type " +
        "FROM socOUT")

    spark.close()

  }

}
