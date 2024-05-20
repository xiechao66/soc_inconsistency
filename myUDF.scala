package com.geely.general

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import java.util

object myUDF {

  /**
   * 检查单体电压是否全部都相等
   *
   * @param cellVol 输入的单体电压数组，Scala Sequence类型
   * @return 返回布尔类型，若一样则返回false，否则返回true
   */
  def voltageAllSame(cellVol: Seq[Double]): Boolean = {
    var lastElement = cellVol.head
    for (vol <- cellVol) {
      if (vol != lastElement)
        return true
      lastElement = vol
    }
    false
  }

  /**
   * 检查是否存在超出有效值范围的单体电压值
   *
   * @param cellVol 输入的单体电压数组，Scala Sequence类型
   * @return 若有则返回false，否则返回true。
   */
  def voltageNormal(cellVol: Seq[Double]): Boolean = {
    for (vol <- cellVol)
      if (vol < 2500 || vol > 4400)
        return false
    true
  }

  /** E225/PA2A
   * 检查是否存在超出有效值范围的单体电压值
   *
   * @param cellVol 输入的单体电压数组，Scala Sequence类型
   * @return 若有则返回false，否则返回true。
   */
  def voltageNormal1(cellVol: Seq[Double]): Boolean = {
    for (vol <- cellVol)
      if (vol < 1500 || vol > 3700)
        return false
    true
  }

  /**
   * 检查温度探针#2 - #34的温度值是否全部都一样。
   *
   * @param cellTemp 输入的温度探针数组，Scala Sequence类型
   * @return 返回布尔类型，若一样则返回false，否则返回true。
   */
//  def tempAllSame(cellTemp: Seq[Double]): Boolean = {
//    var lastElement: Double = cellTemp(1)
//    for (i <- 2 until cellTemp.length) {
//      if (cellTemp(i) != lastElement) return true
//      lastElement = cellTemp(i)
//    }
//    false
//  }
  def tempAllSame(cellTemp: Seq[Double]): Boolean = {
    if (cellTemp.length < 2) {
      return false
    }
    var lastElement: Double = cellTemp(1)

    for (i <- 2 until cellTemp.length) {
      if (cellTemp(i) != lastElement) return true
      lastElement = cellTemp(i)
    }
    false
  }

  /**
   * 检查是否存在超出有效值范围的温度探针值
   *
   * @param cellTemp 输入的温度探针数组，Scala Sequence类型
   * @return 若有则返回false，否则返回true。
   */
  def tempNormal(cellTemp: Seq[Double]): Boolean = {
    for (temp <- cellTemp)
      if (temp < 1 || temp > 125)
        return false
    true
  }

  /** NCM
   * 根据单体电压数组计算所有单体的SOC
   *
   * @param cellVol 输入单体电压数组，Scala Sequence类型
   * @param pack    输入电池包型号，字符串类型
   * @return 返回所有单体SOC
   */
  def arraySOC_NCM(cellVol: Seq[Double], pack: String): Array[Double] = {
    val res = new Array[Double](cellVol.length)
    val ocv = SOCFunc_NCM.searchOCV(pack)
    if (ocv != null) {
      var a1 = .0
      var b1 = .0
      var a2 = .0
      var b2 = .0
      var vol = .0
      for (i <- cellVol.indices) {
        vol = cellVol(i) / 1000
        val ind = util.Arrays.binarySearch(ocv(0), vol) // 二分查找,若存在vol值，返回索引（ind>=0）;否则为（-（插入点的索引）-1）
        if (ind >= 0) res(i) = ocv(1)(ind) // 有对应值
        else {
          if (ind == -1) res(i) = ocv(1)(0)
          else if (ind == -(ocv(0).length + 1)) res(i) = ocv(1)(ocv(0).length - 1) // 100
          else {
            a1 = ocv(0)(-ind - 2)
            b1 = ocv(1)(-ind - 2)
            a2 = ocv(0)(-ind - 1)
            b2 = ocv(1)(-ind - 1)
            res(i) = (b2 - b1) / (a2 - a1) * vol + (b1 * a2 - b2 * a1) / (a2 - a1)
          }
        }
      }
    } else {
      for (i <- cellVol.indices) {
        res(i) = 999.0
      }
    }
    res
  }

  /** LFP
   * 根据单体电压数组计算所有单体的SOC
   *
   * @param cellVol 输入单体电压数组，Scala Sequence类型
   * @param pack    输入电池包型号，字符串类型
   * @return 返回所有单体SOC
   */
  def arraySOC_LFP(cellVol: Seq[Double], pack: String): Array[Double] = {
    val res = new Array[Double](cellVol.length)
    val ocv = SOCFunc_LFP.searchOCV(pack)
    if (ocv != null) {
      var a1 = .0
      var b1 = .0
      var a2 = .0
      var b2 = .0
      var vol = .0
      for (i <- cellVol.indices) {
        vol = cellVol(i) / 1000
        val ind = util.Arrays.binarySearch(ocv(0), vol) // 二分查找,若存在vol值，返回索引（ind>=0）;否则为（-（插入点的索引）-1）
        if (ind >= 0) res(i) = ocv(1)(ind) // 有对应值
        else {
          if (ind == -1) res(i) = ocv(1)(0)
          else if (ind == -(ocv(0).length + 1)) res(i) = ocv(1)(ocv(0).length - 1) // 100
          else {
            a1 = ocv(0)(-ind - 2)
            b1 = ocv(1)(-ind - 2)
            a2 = ocv(0)(-ind - 1)
            b2 = ocv(1)(-ind - 1)
            res(i) = (b2 - b1) / (a2 - a1) * vol + (b1 * a2 - b2 * a1) / (a2 - a1)
          }
        }
      }
    } else {
      for (i <- cellVol.indices) {
        res(i) = 999.0
      }
    }
    res
  }

  /**
   * 求数组中位数
   *
   * @param inputSeq 输入数组，Scala Sequence类型
   * @return 返回Double类型的中位数
   */
  def arrayMedianDouble(inputSeq: Seq[Double]): Double = {
    val num = inputSeq.length
    if (num % 2 == 1) inputSeq(num / 2)
    else (inputSeq(num / 2) + inputSeq(num / 2 - 1)) / 2
  }

  /**
   * 求数组中位数
   *
   * @param inputSeq 输入数组，Scala Sequence类型
   * @return 返回Double类型的中位数
   */
  def arrayMedianShort(inputSeq: Seq[Short]): Int = {
    val num = inputSeq.length
    if (num % 2 == 1) inputSeq(num / 2)
    else (inputSeq(num / 2) + inputSeq(num / 2 - 1)) / 2
  }

  /**
   * 求数组中位数
   *
   * @param inputSeq 输入数组，Scala Sequence类型
   * @return 返回Double类型的中位数
   */
  def arrayMedian(inputSeq: Seq[Double]): Double = {
    val num = inputSeq.length
    if (num % 2 == 1) inputSeq(num / 2)
    else (inputSeq(num / 2) + inputSeq(num / 2 - 1)) / 2
  }

  /**
   * 转换arrayType的类型
   *
   * @param inputSeq 输入数组
   * @return 返回ArrayType(Double)的数组
   */
  def short2Double(inputSeq: Seq[Short]): Seq[Double] = {
    inputSeq.map(_.toDouble)
  }

  /**
   * 转换arrayType的类型
   *
   * @param inputSeq 输入数组
   * @return 返回ArrayType(Double)的数组
   */
  def int2Double(inputSeq: Seq[Int]): Seq[Double] = {
    inputSeq.map(_.toDouble)
  }



  /**
   * 判断HDFS中是否存在该路径
   *
   * @param spark 输入实例化的SparkSession
   * @param path  输入字符串类型的HDFS路径
   * @return 若存在路径，返回true， 否则返回false
   */
  def isExisted(spark: SparkSession, path: String): Boolean = {
    // 取文件系统
    val filePath = new Path(path)
    val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    // 判断路径是否存在，存在=true，不存在=false
    fileSystem.exists(filePath)
  }

  /**
   * 按照10A区间将总电流转换为标志位
   *
   * @param current Double类型的总电流数值
   * @return Int类型的标志位
   */
  def currentFlag(current: Double): Int = {
    val flag = current / 10.0
    if (flag >= 5) 5
    else if (flag < 0) -1
    else flag.toInt
  }

  /**
   * 删除90天之前的特征数据
   *
   * @param spark 输入实例化的SparkSession
   * @param path  输入字符串类型的HDFS路径
   */
  def deleteBefore90(spark: SparkSession, path: String): Unit = {
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileStatus = fileSystem.listStatus(new Path(path))
    val pathList = FileUtil.stat2Paths(fileStatus)
    val num = pathList.length
    if (num > 90) {
      val deleteList = pathList.slice(0, num - 90)
      deleteList.foreach(path => println(path.toString))
      deleteList.map(path => fileSystem.delete(path, true))
    }
  }


  /**
   * 求单体的内阻
   *
   * @param pbpv          单体电压数组
   * @param pbpvLead      下一帧的单体电压数组
   * @param currentDiffer 总电流差
   * @return 内阻数组
   */
  def internalResistance(pbpv: Seq[Double], pbpvLead: Seq[Double], currentDiffer: Double): Array[Double] = {
    var volDiffSingle: Double = .0
    var interR: Double = .0
    var result: Array[Double] = new Array[Double](pbpv.size)

    if (pbpv.size > 118) {
      return result
    }
    if (pbpvLead == null || (currentDiffer == 0.0)) {
      return result
    }
    if (pbpv.size == pbpvLead.size) {
      for (i <- 0 until pbpv.size) {
        volDiffSingle = pbpvLead.apply(i) - pbpv.apply(i)
        interR = volDiffSingle / currentDiffer
        result(i) = interR
      }

      if (iRisSameD((result))) {
        for (i <- 0 until pbpv.size) {
          result(i) = Math.abs(result(i))
        }
        result
      }
      else {
        var result = new Array[Double](pbpv.size)
        result
      }
    }
    else {
      result
    }
  }

  /**
   * 判断内阻值是否都在一个方向
   *
   * @param iResistance 温度探针数组
   * @return 布尔值
   */
  def iRisSameD(iResistance: Array[Double]) = {
    var tempResult = new Array[Boolean](iResistance.size)

    for (i <- 0 until iResistance.size) {
      if (iResistance(i) < 0) tempResult(i) = false
      else tempResult(i) = false
    }
    val result = tempResult.distinct

    if (result.size == 1) true
    else false
  }

  /**
   * 判断单体内阻是否存在零值
   *
   * @param internalR 单体内阻组 Scala Sequence类型
   * @return 返回布尔类型，true表示存在零值。
   */

  def isZero(internalR: Seq[Double]): Boolean = {
    for (i <- 0 until internalR.length)
      if (internalR.apply(i) == 0.0)
        return false
    true
  }


  /**
   * 判断中位数内阻值是否在范围内
   *
   * @param iR_median    单体内阻组 Scala Sequence类型
   * @param declare_type 单体内阻组 Scala String类型
   * @return 返回布尔类型，true表示是否在范围内。
   */
  def iRmedian_range(iR_median: Double, declare_type: String): Boolean = {
    val declare_type1 = List("MR7002BEV03", "HQ7002BEV04", "HQ7002BEV08", "HQ7002BEV11", "HQ7002BEV12",
      "HQ7002BEV05", "HQ7002BEV35", "HQ7002BEV36", "HQ7002BEV56", "HQ7002BEV63", "JHC7002BEV34", "HQ7002BEV45", "HQ7002BEV57",
      "JHC7002BEV25", "HQ7002BEV51", "HQ7002BEV56", "JHC7002BEV26", "JHC7002BEV27", "JHC7002BEV33", "JHC7002BEV34", "HQ7002BEV57",
      "HQ7002BEV66", "HQ7002BEV63", "JHC7002BEV45", "HQ7002BEV72", "JL7002BEV05", "JL7002BEV06", "HQ7002BEV37", "HQ7002BEV39",
      "MR7002BEV22", "MR7002BEV23", "JHC7002BEV38", "JHC7002BEV39", "JHC7002BEV41", "JHC7002BEV46", "HQ7002BEV15", "HQ7002BEV73",
      "JHC7002BEV55", "JHC7002BEV60", "JHC7002BEV51", "JHC7002BEV63", "HQ7003BEV01", "JHC7003BEV02", "JHC7003BEV06", "HQ7003BEV04",
      "JHC7003BEV01", "JHC7003BEV04", "HQ7003BEV03", "JHC7003BEV03", "HQ7002BEV70", "HQ7002BEV77", "MR7002BEV28", "HQ7002BEV76",
      "JHC7002BEV64", "HQ7002BEV68", "JHC7002BEV48", "HQ7002BEV67", "HQ7002BEV84", " JHC7002BEV47", "HQ7002BEV78") // GEV
    val declare_type2 = List("MR7001BEV03", "MR7001BEV04", "MR7001BEV05", "MR7001BEV06", "MR7001BEV08",
      "MR7001BEV11", "MR7001BEV15", "MR7001BEV16", "MR7001BEV17", "MR7001BEV18", "MR7001BEV20") // DC1E
    val declare_type3 = List("HQ7004BEV01") //EX3
    val declare_type4 = List("MR6501DPHEV01") //EX11

    if (declare_type1.contains(declare_type)) {
      if (iR_median < 0.9 && iR_median > 0.3) {
        true
      }
      else false
    }
    else if (declare_type2.contains(declare_type)) {
      if (iR_median <= 0.8 && iR_median >= 0.2) {
        true
      }
      else false
    }
    else if (declare_type3.contains(declare_type)) {
      if (iR_median <= 1 && iR_median >= 0.2) {
        true
      }
      else false
    }
    else if (declare_type4.contains(declare_type)) {
      if (iR_median <= 2 && iR_median >= 0.8) {
        true
      }
      else false
    }
    else false
  }

  /**
   * 求单体的内阻与中位数的差值
   *
   * @param iR_list   单体内阻数组
   * @param iR_median 内阻中位值
   * @return 内阻数组
   */
  def differ_iR(iR_list: Seq[Double], iR_median: Double): Array[Double] = {
    var iR_differ = .0
    val result = new Array[Double](iR_list.size)
    if (iR_list == null || iR_median == 0.0) return result
    for (i <- 0 until iR_list.size) {
      iR_differ = iR_list.apply(i) - iR_median
      result(i) = iR_differ
    }
    result
  }

  /**
   * 根据电池包型号取单体数
   *
   * @description: 字符串截取
   * @param 电池包型号
   * @return 单体数
   */
  def strCutInt(str: String) = {
    val subStr: String = str.split("P").last.split("S")(0).split("s")(0)
    if (subStr == "") 0 else subStr.toInt
  }

  /**
   * E225 根据SOC数据筛选
   *
   */
  def soc_range(declare_type: String, SOC: Int) = {
    val declare_type1 = List("MR7003BEV01", "MR7003BEV07", "MR7003BEV02", "MR7003BEV03", "MR7003BEV06")
    val declare_type2 = List("MR7003BEV05")

    if (declare_type1.contains(declare_type)) {
      if ((SOC <= 100 && SOC >= 95) || (SOC <= 59 && SOC >= 54) || (SOC <= 22 && SOC >= 7) || (SOC <= 2 && SOC >= 0)) {
        true
      }
      else false
    }
    else if (declare_type2.contains(declare_type)) {
      if ((SOC <= 100 && SOC >= 94) || (SOC <= 63 && SOC >= 58) || (SOC <= 22 && SOC >= 7) || (SOC <= 2 && SOC >= 0)) {
        true
      }
      else false
    }
    else true
  }

  /**
   * LFP 根据单体电压数据筛选
   * E225
   * PA2A
   * G636
   */
  def vol_range(declare_type: String, cellVol: Seq[Double]): Boolean = {
    //E225
    val declare_typeE225_1 = List("MR7003BEV01", "MR7003BEV07", "MR7003BEV02", "MR7003BEV03", "MR7003BEV06")  // CATL_92Ah_4P117S,CATL_115Ah_5P110S
    val declare_typeE225_2 = List("MR7003BEV05")  //GK_107Ah_2P118S
    //PA2A
    val declare_typePA2A_1 = List("JL7007BEV01") //GK_110Ah_1P28S
    val declare_typePA2A_2 = List("JL7007BEV02", "JL7007BEV03", "JL7007BEV04") //GK_52Ah_1P104S
    //G636、G733
    val declare_typeG636_G733_1: List[String] = List("HQ6472DCHEV01", "HQ7153DCHEV01") // CATL_60Ah_1P100S
    val declare_typeG636_G733_2: List[String] = List("HQ6472DCHEV03", "HQ7153DCHEV02") // FC_23Ah_1P124S
    val declare_typeG636_G733_3: List[String] = List("HQ6472DCHEV04", "HQ7153DCHEV04") // FC_62Ah_1P100S

    for (vol <- cellVol) {
      if (declare_typeE225_1.contains(declare_type)) {
        if ((vol <= 3700 && vol >= 3332) || (vol <= 3317 && vol >= 3295) || (vol <= 3286 && vol >= 3206) || (vol <= 3200 && vol >= 1500)) {
        }
        else return false
      }
      else if (declare_typeE225_2.contains(declare_type)) {
        if ((vol <= 3700 && vol > 3334) || (vol <= 3331 && vol >= 3305) || (vol < 3294 && vol >= 3212) || (vol <= 3208 && vol >= 1500)) {
        }
        else return false
      }
      //PA2A
      else if (declare_typePA2A_1.contains(declare_type)) {
        if ((vol <= 3700 && vol >= 3328) || (vol <= 3321 && vol >= 3294) || (vol <= 3281 && vol >= 3200) || (vol <= 3193 && vol >= 1500)) {
        }
        else return false
      }
      else if (declare_typePA2A_2.contains(declare_type)) {
        if ((vol <= 3700 && vol > 3334) || (vol < 3332 && vol >= 3305) || (vol <= 3286 && vol >= 3213) || (vol <= 3206 && vol >= 1500)) {
        }
        else return false
      }
      // G636、G733
      else if (declare_typeG636_G733_1.contains(declare_type)) {
        if ((vol <= 3700 && vol >= 3325) || (vol <= 3309 && vol >= 3289) || (vol <= 3263 && vol >= 1500)) {
        }
        else return false
      }
      else if (declare_typeG636_G733_2.contains(declare_type)) {
        if ((vol <= 3700 && vol >= 3336) || (vol <= 3330 && vol >= 3295) || (vol <= 3287 && vol >= 3207) || (vol <= 3201 && vol >= 1500)) {
        }
        else return false
      }
      else if (declare_typeG636_G733_3.contains(declare_type)) {
        if ((vol <= 3700 && vol >= 3337) || (vol <= 3322 && vol >= 3295) || (vol <= 3286 && vol >= 1500)) {
        }
        else return false
      }
      false
    }
    true
  }

  /**
   * E225 计算两个伪静置片段之间的单体SOC
   *
   */
  def soc_compute(cellsoc: Seq[Double], Ah: Double) = {
    val result = new Array[Double](cellsoc.size)
    for (i <- 0 until cellsoc.size) {
      result(i) = cellsoc.apply(i) - Ah
    }
    result
  }

  /**
   * E225 计算两个伪静置片段之间的安时
   *
   */
  def Ah_compute(current: Double, timediff: Int, declare_type: String) = {
    val declare_type1 = List("MR7003BEV01", "MR7003BEV07")
    val declare_type2 = List("MR7003BEV02", "MR7003BEV03", "MR7003BEV06")
    val declare_type3 = List("MR7003BEV05")
    var result = .0
    if (declare_type1.contains(declare_type)) {
      val Q = 92
      result = current * timediff / Q / 3600
    }
    else if (declare_type2.contains(declare_type)) {
      val Q = 115
      result = current * timediff / Q / 3600
    }
    else if (declare_type3.contains(declare_type)) {
      val Q = 107
      result = current * timediff / Q / 3600
    }
    result

  }

  /**
   * 判断整包欠压、过压
   *
   */

  def judge_vol_ncm(cellVol: Seq[Double]) = {
    var vol_flag: Array[Int] = new Array[Int](cellVol.size)
    for (i <- 0 until cellVol.size) {
      if (cellVol(i) <= 2500.0) vol_flag(i) =  1
      else if (cellVol(i) >=4500.0 ) vol_flag(i) =  2
//      else vol_flag = vol_flag :+0
    }
    val result = vol_flag.distinct
    result
    }

  def judge_vol_lfp(cellVol: Seq[Double]) = {
    var vol_flag: Array[Int] = new Array[Int](cellVol.size)
    for (i <- 0 until cellVol.size) {
      if (cellVol(i) <= 2000.0) vol_flag(i) =  1
      else if (cellVol(i) >=3700.0 ) vol_flag(i) =  2
      //      else vol_flag = vol_flag :+0
    }
    val result = vol_flag.distinct
    result
  }

  /**
   * soc偏差正负标签
   * @return soc标签
   */
  def judge_soc(cellsoc: Seq[Double]) = {
    var soc_flag: Array[Int] = new Array[Int](cellsoc.size)
    for (i <- 0 until cellsoc.size) {
      if (cellsoc(i) < 0.0) {
        soc_flag(i) =  -1
      }
      else if (cellsoc(i) > 0.0 ) soc_flag(i) =  1
//      else soc_flag = soc_flag :+ "0"
    }
  soc_flag
  }


  /**
   * 求单体的SOC与中位数的差值
   *
   * @param iR_list   单体SOC数组
   * @param iR_median SOC中位值
   * @return 数组
   */
  def differ_SOC(SOC_list: Seq[Double], SOC_median: Double): Array[Double] = {
    var SOC_differ = .0
    val result = new Array[Double](SOC_list.size)
    if (SOC_list == null || SOC_median == 0.0) return result
    for (i <- 0 until SOC_list.size) {
      SOC_differ = SOC_list.apply(i) - SOC_median
      result(i) = SOC_differ
    }
    result
  }


}





