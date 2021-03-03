package com.KPI5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.lang.Long
import java.lang.Double

object Agegroupacceptencepercentage {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:/software/scala/hadoop")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")
    val spark = SparkSession
      .builder()
      .appName("aadharCardAnlysis")
      .master("local")
      .getOrCreate()
    val csvData = spark.read.csv("D:\\scala\\resources\\aadhaar_data.csv")

    val csvData2 = csvData.withColumnRenamed("_c0", "Date")
      .withColumnRenamed("_c1", "Registrar")
      .withColumnRenamed("_c2", "PrivateAgency")
      .withColumnRenamed("_c3", "State")
      .withColumnRenamed("_c4", "District")
      .withColumnRenamed("_c5", "SubDistrict")
      .withColumnRenamed("_c6", "PinCode")
      .withColumnRenamed("_c7", "Gender")
      .withColumnRenamed("_c8", "Age")
      .withColumnRenamed("_c9", "AadharGenerated")
      .withColumnRenamed("_c10", "Rejected")
      .withColumnRenamed("_c11", "MobileNo")
      .withColumnRenamed("_c12", "EmailId")
    csvData2.createOrReplaceTempView("Aadhar")

    val df = csvData2.filter(csvData2("AadharGenerated") > 0)
    val totalAadharGen = df.agg(sum("AadharGenerated")).collect()
    val age0to10 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") <= 10).agg(sum("AadharGenerated")).collect()
    val age10to20 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 10).filter(csvData2("Age") <= 20).agg(sum("AadharGenerated")).collect()
    val age20to30 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 20).filter(csvData2("Age") <= 30).agg(sum("AadharGenerated")).collect()
    val age30to40 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 30).filter(csvData2("Age") <= 40).agg(sum("AadharGenerated")).collect()
    val age40to50 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 40).filter(csvData2("Age") <= 50).agg(sum("AadharGenerated")).collect()
    val age50to60 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 50).filter(csvData2("Age") <= 60).agg(sum("AadharGenerated")).collect()
    val age60to70 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 60).filter(csvData2("Age") <= 70).agg(sum("AadharGenerated")).collect()
    val age70to80 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 70).filter(csvData2("Age") <= 80).agg(sum("AadharGenerated")).collect()
    val age80to90 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 80).filter(csvData2("Age") <= 90).agg(sum("AadharGenerated")).collect()
    val age90to100 = csvData2.filter(csvData2("AadharGenerated") > 0).filter(csvData2("Age") > 90).filter(csvData2("Age") <= 100).agg(sum("AadharGenerated")).collect()
    println("acceptance percentage between age groupbe 0 to 10  : " + (age0to10(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 10 to 20 : " + (age10to20(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 20 to 30 : " + (age20to30(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 30 to 40 : " + (age30to40(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 40 to 50 : " + (age40to50(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 50 to 60 : " + (age50to60(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 60 to 70 : " + (age60to70(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 70 to 80 : " + (age70to80(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 80 to 90 : " + (age80to90(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)
    println("acceptance percentage between age groupbe 90 to 100 : " + (age90to100(0).getDouble(0) / totalAadharGen(0).getDouble(0)) * 100)

    

  }
}