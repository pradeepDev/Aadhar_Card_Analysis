package com.KPI5

import org.apache.spark.sql.SparkSession

object aadhardgeneratedforfemale {
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

    val df = spark.sql("select  sum(AadharGenerated) as totalAadharGenerated , state from Aadhar   group by state")
    df.createTempView("TotalAadhargenerayedByState")

    val df2 = spark.sql("select  sum(AadharGenerated) as femaleAadharGenerated , state from Aadhar where Gender=='F'  group by state")
    df2.createTempView("TotalAadhargeneratedforFeMale")

    val df3 = spark.sql("select ((TotalAadhargeneratedforFeMale.femaleAadharGenerated/TotalAadhargenerayedByState.totalAadharGenerated) *100) As PercetFeMaleAadharGen,TotalAadhargenerayedByState.state from TotalAadhargenerayedByState, TotalAadhargeneratedforFeMale where TotalAadhargenerayedByState.state = TotalAadhargeneratedforFeMale.state order by PercetFeMaleAadharGen DESC limit 3 ")
  
    df3.show(df3.count().toInt, false)
  }
}