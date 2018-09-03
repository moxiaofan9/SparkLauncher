package com.haizhi.graph.hive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by wangxy on 2018/4/13.
  */


object SparkOnHive {

  val cf = "relationships"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("yarn")
      .appName("Spark On Hive")
      //      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    //spark.sparkContext.addFile("/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources/hive-site.xml")

    //spark.sparkContext.addFile(args(1))
    println("spark.sql.warehouse.dir=" + spark.sparkContext.getConf.get("hive.warehouse.dir", "11"))

    /*    sql("select count(*) from enterprise_data").show()*/

    /*    val mergeResult: DataFrame = spark.read.json("/user/graph-algorithm/online/person_merge")
        mergeResult.printSchema()
        mergeResult.createOrReplaceTempView("tmp")*/

    val result = testScanFilter(spark, "enterprise:officer", Array("object_key", "from_key", "to_key"), args(0)) //.union(mergeResult)
    result.printSchema()
    result.createOrReplaceTempView("tmp")


    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    import spark.implicits._
    import spark.sql
    sql("use test")

    sql(
      s"""
         |insert into table test_crm partition(p_date, p_hour)
         |select object_key, from_key as object_from,to_key as object_to, ctime, p_date, p_hour
         |from tmp
         """.stripMargin)

    /*    sql(
          s"""
             |insert into table test_crm partition(p_date, p_hour)
             |select object_key, _from as object_from, _to as object_to, ctime, date(ctime), hour(ctime)
             |from tmp"
                    """.stripMargin)*/

    /*    mergeResult.write.partitionBy("p_date").partitionBy("p_hour")
          .mode(SaveMode.Overwrite)
          .format("hive")
          .saveAsTable("test_crm")*/

    val time1 = System.currentTimeMillis()
    val k: DataFrame = sql("select count(*) as sum, p_date, p_hour from test_crm group by p_date, p_hour order by p_date, p_hour")
    k.rdd.collect().foreach(println(_))
    val time2 = System.currentTimeMillis()
    println(time2 - time1)

    sql("drop table test_crm")
    spark.stop()
  }

  def testScanFilter(spark: SparkSession, tableName: String, qf: Array[String], path: String): DataFrame = {
    val scan = new Scan()
    scan.setBatch(1000)
    scan.setCacheBlocks(false)
    //    scan.setMaxVersions()
    qf.foreach(field => scan.addColumn(cf.getBytes, field.getBytes))
    val conf = HBaseConfiguration.create()
    /*conf.set("hbase.zookeeper.quorum", "192.168.1.16,192.168.1.17,192.168.1.18")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")*/
    conf.addResource(new Path(path))
    conf.setInt("hbase.rpc.timeout", 200000)
    conf.setInt("hbase.client.scanner.timeout.period", 200000)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    val dates = Array("2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08")

    val row: RDD[Row] = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map { case (_, result) => {
      val f: Seq[String] = qf.map(field => Bytes.toString(result.getValue(cf.getBytes, field.getBytes))).toSeq
      val date = dates(new Random().nextInt(4))
      val hour = new Random().nextInt(24)
      val ctime = s"$date $hour:00:00"
      Row.fromSeq(f ++ Array(ctime, date, hour.toString))
    }
    }

    val struct: Seq[StructField] = qf.map(field => StructField(field, StringType, nullable = true))
    val schema = StructType(struct ++
      Array(StructField("ctime", StringType, nullable = true),
        StructField("p_date", StringType, nullable = true),
        StructField("p_hour", StringType, nullable = true)))
    spark.createDataFrame(row, schema)
  }


}
