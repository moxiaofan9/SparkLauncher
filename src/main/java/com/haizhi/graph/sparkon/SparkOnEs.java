package com.haizhi.graph.sparkon;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * Created by wangxy on 2018/8/7.
 */


public class SparkOnEs {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark To ES")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.rdd.compress", "true")
                .config("es.index.auto.create", "true")
                .config("es.nodes", "192.168.1.49,192.168.1.51,192.168.1.52")
                .config("es.port", "9200")
                //        .config("es.nodes.wan.only", "false")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        long tim1 = System.currentTimeMillis();
        JavaRDD<String> javaRDD = jsc.textFile(args[0]).repartition(40);
        long time2 = System.currentTimeMillis();
        System.out.println(time2 - tim1);

        JavaEsSpark.saveJsonToEs(javaRDD, "wangxy/Person1", ImmutableMap.of("es.mapping.id", "_key"));
        System.out.println(System.currentTimeMillis() - time2);

    }
}
