package com.haizhi.graph.hive

import org.apache.spark.sql.SparkSession

/**
  * Created by wangxy on 2018/7/5.
  */


object SparkSqlOnHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSql On Hive")
      .master("spark://192.168.1.16:7077")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    val hiveSql =
      s"""
         |SELECT ta.name,
         |       ta.statTime,
         |       sum(ta.num) AS `count(1)`
         |FROM
         |  (SELECT t.company_name AS name,
         |          date(t.filing_date) AS statTime,
         |          count(1) AS num
         |   FROM to_defendant_list t
         |   WHERE t.filing_date > date_sub(current_date(), 90)
         |   GROUP BY t.company_name,
         |            date(t.filing_date)
         |   UNION ALL SELECT t1.company_name AS name,
         |                    date(t1.filing_date) AS statTime,
         |                    count(1) AS num
         |   FROM to_plaintiff_list t1
         |   WHERE t1.filing_date > date_sub(current_date(), 90)
         |   GROUP BY t1.company_name,
         |            date(t1.filing_date)) ta
         |GROUP BY ta.name,
         |         ta.statTime
       """.stripMargin
    sql("use crm_dev2")
    val result = sql(hiveSql)
    result.printSchema()
    result.show()

    spark.stop()
  }
}
