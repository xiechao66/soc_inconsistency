package com.geely.main.Temp

import com.geely.general.{dateFunc, myUDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Tempfeature {


  def main(args: Array[String]): Unit = {

    // 配置
    val spark = SparkSession.builder.appName("temp feature manual hive").enableHiveSupport.getOrCreate
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", 1000)
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    // UDF
    val arrayMedian = udf((arr: Seq[Double]) => myUDF.arrayMedian(arr))
    val voltageNormal = udf((cellVol: Seq[Double]) => myUDF.voltageNormal(cellVol))
    val voltageAllSame = udf((cellVol: Seq[Double]) => myUDF.voltageAllSame(cellVol))
    val tempAllSame = udf((cellTemp: Seq[Double]) => myUDF.tempAllSame(cellTemp))
    val tempNormal = udf((cellTemp: Seq[Double]) => myUDF.tempNormal(cellTemp))
    val int2Double = udf((cellVol: Seq[Int]) => myUDF.int2Double(cellVol)) //转换arrayType的类型
    val short2Double = udf((cellTemp: Seq[Short]) => myUDF.short2Double(cellTemp))


    // 输入参数：soc表示仅输出soc特征，temp表示仅输出温度特征，为空则同时输出
    val pt = args(0)
    val pt2 = dateFunc.chargeDateFormat(pt, "yyyyMMdd", "yyyy-MM-dd")
    val cell = args(1)
    val mincell = args(2)
    val minprobe = args(3)

    // 读取数据
    val origin = spark.sql(
      "SELECT " +
        "vin AS Vin, " +
        "sending_time AS sendingTime, " +
        "CAST(vehicle_bir AS INT) AS bir, " +
        "CAST(vehicle_total_current AS INT) AS total_current, " +
        "CAST(vehicle_total_voltage AS INT) AS total_voltage, " +
        "temp_units AS pbpt_data, " +
        "total_pb_probes AS temp_probes, " +
        "extreme_max_vofcells AS max_vofcells, " +
        "extreme_min_vofcells AS min_vofcells, " +
        "vol_units AS pbpv_data, " +
        "batt_total_cells AS cells, " +
        "rowkey, " +
        "CAST(vehicle_mileage AS INT) AS mileage, " +
        "CAST(vehicle_charging_status AS INT) AS charging_status, " +
        "dr_motor_voltage AS driveMotorData " +
        "FROM viree_forward_search_dev.fs_ods_new_gb_vehicle_full_data_di " +
        s"WHERE pt IN ($pt)"
    ).where(col("sendingTime") >= (pt2 + " 00:00:00") && col("sendingTime") <= (pt2 + " 23:59:59"))
    // AND declare_type IN ('HQ7152DCHEV01','HQ7152DCHEV02', 'JL7000BEV02', 'JL7000BEV03', 'JL7000BEV04', 'JL7000BEV05', 'JL7000BEV08', 'JL7000BEV09', 'MR6501DCHEV01', 'JL6482DCHEV02', 'JL6482DCHEV03')

    // 数据预处理
    val processed = origin.withColumn("timeStamp", unix_timestamp(col("sendingTime"), "yyyy-MM-dd HH:mm:ss").cast("long"))
      .withColumn("mileage", col("mileage") / 10)
      .withColumn("total_current", col("total_current") / 10 - 1000)
      .withColumn("total_voltage", col("total_voltage") / 10)
      .withColumn("date", split(col("sendingTime"), " ")(0))

    //-------------------------------------------------------温度--------------------------------------------------------
    // 数据筛选：
    //a)	总电压total_voltage != 0；
    //b)	充电状态charging_status == 3；
    //c)	温度探针数组pbpt_data长度 == 温度探针数temp_probes；
    //d)	温度探针数组中所有温度处于(-40, 85)℃且第2个探针开始往后不得全部相同；
    //e)	去除绝缘阻值bir、单体电压pbpv_data、总电流total_current、驱动电机电压driveMotorData均一直发相同数值的数据（目的是去除BMS已经休眠，T-BOX未休眠仍在发的重复数据）。
    val filtered = processed.where(col("charging_status") === 3)
      .where(col("total_voltage") =!= 0)
      .where(col("max_vofcells") <= 4400)
      .where(col("min_vofcells") >= 2500)
      .where(col("total_current") >= -2 && col("total_current") <= 2)
      .dropDuplicates("Vin", "bir", "total_current", "pbpv_data", "driveMotorData", "mileage")
      .where(col("cells") >= mincell.toInt)
      .where(col("temp_probes") >= minprobe.toInt)
      .where(size(col("pbpt_data")) === col("temp_probes"))
      .where(size(col("pbpv_data")) === col("cells"))
      .withColumn("pbpv_data", int2Double(col("pbpv_data")))
      .withColumn("pbpt_data", short2Double(col("pbpt_data")))
      .where(voltageAllSame(col("pbpv_data")) === true)
      .where(voltageNormal(col("pbpv_data")) === true)
      .where(tempAllSame(col("pbpt_data")) === true) //温度从第2个探针后不相同
      .where(tempNormal(col("pbpt_data")) === true) //（-40，85）
      .drop("driveMotorData", "bir")


    // 获得车架号对应的PACK型号和OCV
    val staticResWithInfo_temp = filtered
      .withColumn("medTemp", arrayMedian(sort_array(col("pbpt_data"))))
      .drop("pbpv_data")

    val tempOUT: DataFrame = staticResWithInfo_temp.select(col("Vin") +:
      col("sendingTime") +:
      col("date") +:
      col("timeStamp") +:
      col("mileage") +:
      col("rowkey") +:
      col("medTemp") +:
      (0 until cell.toInt).map(i => (col("pbpt_data")(i) - col("medTemp")).alias(s"$i")): _*
    ).groupBy("Vin", "date").agg(
      first("mileage").alias("startMileage"),
      last("mileage").alias("endMileage") +:
        count("rowkey").alias("num") +:
        (0 until cell.toInt).map(i => avg(col(s"$i")).alias(s"avg${i + 1}")): _*
    )

    tempOUT.createTempView("tempOUT")
    spark.sql(
      s" INSERT OVERWRITE TABLE viree_forward_search_dev.fs_dws_temp_feature_di PARTITION (pt=$pt) " +
        "SELECT * FROM tempOUT")

    spark.close()

  }


}
