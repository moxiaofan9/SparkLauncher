package com.haizhi.graph.sparklauncher;

import com.haizhi.graph.sparkon.SparkOnHBase;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Created by wangxy on 2018/3/9.
 */


public class SparkLauncherMain {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
 //               .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.rdd.compress", "true")
                .getOrCreate();
/*        JavaRDD<String> rdd = spark.read().textFile("hdfs://192.168.1.16:8020/user/wangxy/part").javaRDD();
        long sum = rdd.count();
        System.out.println("sum "+ sum);*/

        try {
            SparkOnHBase.getHBaseData(spark, "hbase_test", "Company", "","");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
