package com.haizhi.graph.sparkon;

import org.apache.spark.sql.SparkSession;

/**
 * Created by wangxy on 2018/4/13.
 */


public class SparkOnHive {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .appName("Spark On Hive")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("use test");
        spark.sql("select count(*) from enterprise_data").show();

        spark.stop();
    }
}
