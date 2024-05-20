package com.geely.main.soc.v3

import com.geely.general.dateFunc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable

/**
 * update: 1. 20-21, 新增温度输入和温度输出   2. 56-80，70行改为2
 * update: 3. 23-24, 新增内阻输入和内阻输出   4. 87-111
 * update: 5. SOC/温度/内阻 拆开
 */
object SOC_carFilter {

  def main(args: Array[String]): Unit = {

    // 外部输入参数
    val cellNum: String = args(0)
    val outputPath: String = args(1)

    // 配置f
    val spark: SparkSession = SparkSession.builder.appName("car filter").enableHiveSupport().getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    // 前42天数据
    //    val pt1 = dateFunc.getBeforeToday("yyyyMMdd", -43).toInt
//    val pt2: Int = dateFunc.getBeforeToday("yyyyMMdd", -1).toInt
    val pt2: String = args(2)
    print(pt2)

    val pt1: Int = dateFunc.getBeforeDate("yyyyMMdd", pt2, -43).toInt

      // 读取数据
      val origin: DataFrame = spark.sql(
        "SELECT DISTINCT * " +
          "FROM viree_forward_search_dev.fs_dws_soc_feature_v3_di " +
          s"WHERE pt <($pt2)"
      ).drop("pt")

    // define UDF to calculate max of an array
      val maxOfArray = udf((arrayCol: Seq[Double]) => arrayCol.max)
      val processed: DataFrame = origin.withColumn("maxDiff", maxOfArray(col("delta_soc")))

      // ----------------------------------------车辆过滤----------------------------------------------------------
      // 1.该车辆是否存在最大偏差≥5%的天数≥3天；OR
      // 2.存在△Max|SOC|≥10%（TBD）

    val processed_42: Dataset[Row] = processed.where(col("pt") >= s"$pt1")

      val warnCars: DataFrame = processed_42.where(col("maxDiff") >= 5)
        .groupBy("vin").agg(
        count("date").alias("warn"),
        max("maxDiff").alias("max_diff")
      )
        .where((col("warn") >= 3) || (col("max_diff") >= 10))

      // 输出
      val carFilterOUT =
        processed.join(warnCars, Seq("vin"), "inner")
//              .repartition(1)
//              .write.mode("overwrite").format("json").save(outputPath)

      carFilterOUT.createTempView("carFilterOUT")
      spark.sql(
        s" INSERT OVERWRITE TABLE viree_forward_search_dev.fs_dws_soc_carFilter_v3_wi PARTITION (pt=$pt2) " +
          "SELECT DISTINCT vin,date,startMileage,endMileage,num," +
          "delta_soc, maxDiff, car_type " +
          "FROM carFilterOUT")

    spark.close()

  }

}
