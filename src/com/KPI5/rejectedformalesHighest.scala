package com.KPI5

import org.apache.spark.sql.SparkSession

object rejectedformalesHighest {
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
    df3.createTempView("top3districtFeMaleAadhargen")

    val df4 = spark.sql("select  sum(Rejected) as totalRejected , District,state from Aadhar where state in (select State from top3districtFeMaleAadhargen) and Gender=='M'   group by District,state")
    df4.createTempView("TotalAadharRejectedformale")

    val df5 = spark.sql("select  sum(AadharGenerated) as totalAadharGenerated ,District,state from Aadhar where state in (select State from top3districtFeMaleAadhargen) group by District,state")
    df5.createTempView("TotalAadharGeneratedByDistrict")

    val f6 = spark.sql("select  dense_rank() over (order by TotalAadharGeneratedByDistrict.state) as rank,((TotalAadharRejectedformale.totalRejected/TotalAadharGeneratedByDistrict.totalAadharGenerated)*100) as rejectedAadharmale  ,TotalAadharGeneratedByDistrict.District as district , TotalAadharRejectedformale.state as state from TotalAadharRejectedformale,TotalAadharGeneratedByDistrict where TotalAadharGeneratedByDistrict.District = TotalAadharRejectedformale.District order by rejectedAadharmale DESC")
    f6.createOrReplaceTempView("DictrictwiseCountAadhar")
    var i = 1

    val df00 = spark.sql(s"select rank,rejectedAadharmale,district,state from DictrictwiseCountAadhar where rank ='${i}' order by rejectedAadharmale desc limit 3")
    i += 1
    val df01 = spark.sql(s"select rank,rejectedAadharmale,district,state from DictrictwiseCountAadhar where rank ='${i}' order by rejectedAadharmale desc limit 3")
    i += 1
    val df02 = spark.sql(s"select rank,rejectedAadharmale,district,state from DictrictwiseCountAadhar where rank ='${i}' order by rejectedAadharmale desc limit 3")
    i += 1

    val dfn = df00.union(df01)
    val dfl = dfn.union(df02)
    dfl.show(dfl.count.toInt, false)
  }
}