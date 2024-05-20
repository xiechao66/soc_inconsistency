package com.geely.general

import org.apache.spark.sql.types.{StructField, _}

object schemas {

	def voltageStruct: StructType = {
		val pbpvType = StructType(Seq(
			StructField("units", ArrayType(DoubleType), nullable = true),
			StructField("cells", IntegerType, nullable = true)
		))
		val dataListType = StructType(Seq(StructField("drivingMotorVoltage", DoubleType, nullable = true)))
		val motorType = StructType(Seq(StructField("dataList", ArrayType(dataListType), nullable = true)))
		StructType(Seq(
			StructField("Vin", StringType, nullable = false), 
			StructField("sendingTime", StringType, nullable = false), 
			StructField("timeStamp", LongType, nullable = true), 
			StructField("bir", DoubleType, nullable = true), 
			StructField("total_current", DoubleType, nullable = true), 
			StructField("total_voltage", DoubleType, nullable = true), 
			StructField("max_vofcells", IntegerType, nullable = true), 
			StructField("min_vofcells", IntegerType, nullable = true), 
			StructField("pbpv_data", ArrayType(pbpvType), nullable = false),
			StructField("rowkey", StringType, nullable = true), 
			StructField("mileage", IntegerType, nullable = true), 
			StructField("charging_status", IntegerType, nullable = true),
			StructField("driveMotorData", motorType, nullable = false)
		))
	}


	def tempStruct: StructType = {
		val pbptType = StructType(Seq(
			StructField("units", ArrayType(DoubleType), nullable = true),
			StructField("totalPBProbes", IntegerType, nullable = true)
		))
		val dataListType = StructType(Seq(StructField("drivingMotorVoltage", DoubleType, nullable = true)))
		val motorType = StructType(Seq(StructField("dataList", ArrayType(dataListType), nullable = true)))
		StructType(Seq(
			StructField("Vin", StringType, nullable = false),
			StructField("sendingTime", StringType, nullable = false),
			StructField("timeStamp", LongType, nullable = true),
			StructField("bir", DoubleType, nullable = true),
			StructField("total_current", DoubleType, nullable = true),
			StructField("total_voltage", DoubleType, nullable = true),
			StructField("pbpt_data", ArrayType(pbptType), nullable = false),
			StructField("rowkey", StringType, nullable = true),
			StructField("mileage", IntegerType, nullable = true),
			StructField("charging_status", IntegerType, nullable = true),
			StructField("driveMotorData", motorType, nullable = false)
		))
	}


	def featureStruct: StructType = {
		val pbpvType = StructType(Seq(
			StructField("units", ArrayType(DoubleType), nullable = true),
			StructField("cells", IntegerType, nullable = true)
		))
		val pbptType = StructType(Seq(
			StructField("units", ArrayType(DoubleType), nullable = true),
			StructField("totalPBProbes", IntegerType, nullable = true)
		))
		val dataListType = StructType(Seq(StructField("drivingMotorVoltage", DoubleType, nullable = true)))
		val motorType = StructType(Seq(StructField("dataList", ArrayType(dataListType), nullable = true)))

		StructType(Seq(
			StructField("Vin", StringType, nullable = false),
			StructField("sendingTime", StringType, nullable = false),
			StructField("timeStamp", LongType, nullable = true),
			StructField("bir", DoubleType, nullable = true),
			StructField("total_current", DoubleType, nullable = true),
			StructField("total_voltage", DoubleType, nullable = true),
			StructField("max_vofcells", IntegerType, nullable = true),
			StructField("min_vofcells", IntegerType, nullable = true),
			StructField("pbpv_data", ArrayType(pbpvType), nullable = false),
			StructField("pbpt_data", ArrayType(pbptType), nullable = false),
			StructField("rowkey", StringType, nullable = true),
			StructField("mileage", IntegerType, nullable = true),
			StructField("charging_status", IntegerType, nullable = true),
			StructField("driveMotorData", motorType, nullable = false),
			StructField("totalPBProbes", IntegerType, nullable = false),
			StructField("cells", IntegerType, nullable = true),
			StructField("SOC", IntegerType, nullable = true),
			StructField("longitude", DoubleType, nullable = true),
			StructField("latitude", DoubleType, nullable = true)
		))
	}


}
