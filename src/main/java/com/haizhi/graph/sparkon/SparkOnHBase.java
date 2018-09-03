package com.haizhi.graph.sparkon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wangxy on 2018/3/12.
 */


public class SparkOnHBase {
    public static Configuration conf;
    public static String qf = "objects";

    static {
        conf = HBaseConfiguration.create();
/*        conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.16,192.168.1.17,192.168.1.18");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");*/
        conf.addResource(new Path("/usr/hdp/2.6.1.0-129/hbase/conf/hbase-site.xml"));
        conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 200000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 200000);
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
    }

    private static String convertScanToString(Scan scan) throws IOException {
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        return Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
    }

    public static void getHBaseData(SparkSession spark, String namespace, String tableName, String family, String column) throws IOException {
        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes(family));
//        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        conf.set(TableInputFormat.INPUT_TABLE, namespace + ":" + tableName);
        conf.set(TableInputFormat.SCAN, convertScanToString(scan));

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        List<Tuple2<ImmutableBytesWritable, Result>> rddList = hbaseRDD.take(10);
        for (int i = 0; i < rddList.size(); i++) {
            Tuple2<ImmutableBytesWritable, Result> t2 = rddList.get(i);
            ImmutableBytesWritable key = t2._1();
            Iterator<Cell> it = t2._2().listCells().iterator();
            while (it.hasNext()) {
                Cell c = it.next();
                String familys = Bytes.toString(CellUtil.cloneFamily(c));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
                String value = Bytes.toString(CellUtil.cloneValue(c));
                Long tm = c.getTimestamp();
                System.out.println(" Family=" + familys + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm);
            }
        }

        JavaRDD<Tuple2<String, String>> rdd1 = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                Result result = v1._2();
                String object_key = Bytes.toString(result.getValue(Bytes.toBytes(qf), Bytes.toBytes("object_key")));
                String name = Bytes.toString(result.getValue(Bytes.toBytes(qf), Bytes.toBytes("from_key")));
                return new Tuple2<>(object_key, name);
            }
        });

        System.out.println("rdd1 count " + rdd1.count());
        rdd1.take(10).forEach(k -> {
            System.out.println(k._1() + " " + k._2());
        });

        JavaPairRDD<String, String> rdd2 = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                Result result = immutableBytesWritableResultTuple2._2();
                String object_key = Bytes.toString(result.getValue(Bytes.toBytes(qf), Bytes.toBytes("object_key")));
                String name = Bytes.toString(result.getValue(Bytes.toBytes(qf), Bytes.toBytes("from_key")));
                return new Tuple2<>(object_key, name);
            }
        });

        System.out.println("rdd2 count " + rdd2.count());
        rdd2.take(10).forEach(k -> {
            System.out.println(k._1() + " " + k._2());
        });

        JavaPairRDD<String, String> rdd3 = rdd2;


        JavaPairRDD<String, String> result = rdd2.union(rdd3).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("result " + result.count());
        result.take(10).forEach(k -> {
            System.out.println(k._1() + " " + k._2());
        });
    }

}
