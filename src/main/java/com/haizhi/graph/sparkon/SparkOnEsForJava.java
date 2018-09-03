package com.haizhi.graph.sparkon;

import com.alibaba.fastjson.JSON;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import java.util.Map;

/**
 * Created by wangxy on 2018/8/7.
 */


public class SparkOnEsForJava {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark On ES By JavaAPI")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.rdd.compress", "true")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> javaRDD = jsc.textFile(args[0]).repartition(Integer.parseInt(args[1]));
//        String url = "192.168.1.49,192.168.1.51,192.168.1.52:9300";
        String index = "wangxy";
        String type = "Person1";

        long time = System.currentTimeMillis();

        javaRDD.foreachPartition(t -> {
            int i = 0;
            TransportClient client = ESTransportClient.getInstance();
            if (client == null) {
                System.out.println("client is null");
            }
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            while (t.hasNext()) {
                i++;
                String line = t.next();
                Map<String, Object> map = JSON.parseObject(line);
                UpdateRequestBuilder urb = client.prepareUpdate(index, type, map.get("_key").toString())
                        .setDocAsUpsert(true).setDoc(map);
                bulkRequest.add(urb);
                if (i % 2000 == 0) {
                    BulkResponse bulkResponse = bulkRequest.get();
                    if (!bulkResponse.hasFailures()) {
                        System.out.println("i=" + i);
                    } else {
                        System.out.println("");
                    }
                }
            }

            BulkResponse bulkResponse = bulkRequest.get();
            if (!bulkResponse.hasFailures()) {
                System.out.println("1");
            } else {
                System.out.println("");
            }

            /*if (client != null) {
                client.close();
            }*/
        });
        System.out.println(System.currentTimeMillis() - time);
    }

}
