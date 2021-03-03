package com.KPI2

import org.apache.spark.sql.SparkSession

object numberOFStatesDistrictSubDisrict {
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

    spark.sql("select count(distinct(state)) from Aadhar").show
    val districtCount = spark.sql("select count(distinct(district)), State from Aadhar group by state")
    districtCount.show(districtCount.count.toInt, false)
    val SubdistrictCount = spark.sql("select count(distinct(SubDistrict)),district, State from Aadhar  group by state,district order by state,district asc") //group by state order by state ASC,district ASC" )
    SubdistrictCount.show(SubdistrictCount.count.toInt, false)
  }
}