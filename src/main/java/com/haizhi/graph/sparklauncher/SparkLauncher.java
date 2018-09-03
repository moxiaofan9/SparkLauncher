package com.haizhi.graph.sparklauncher;

import org.apache.spark.launcher.SparkAppHandle;

import java.util.HashMap;

/**
 * Created by wangxy on 2018/7/30.
 */


public class SparkLauncher {
    public static void main(String[] args) throws Exception {
        System.setProperty("SPARK_HOME", "/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7");

        String filePath = "/Users/haizhi/IdeaProjects/lab/lab-hdp/target";
        //"/Users/haizhi/IdeaProjects/SparkLauncher/target/";

        HashMap<String, String> env = new HashMap<String, String>(2);
        //提交服务器路径地址
/*        env.put("HADOOP_CONF_DIR", "/Users/haizhi/IdeaProjects/lab/lab-hdp/src/main/resource");
        env.put("YARN_CONF_DIR", "/Users/haizhi/IdeaProjects/lab/lab-hdp/src/main/resource");*/
        env.put("HADOOP_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");
        env.put("YARN_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");

        SparkAppHandle handle = new org.apache.spark.launcher.SparkLauncher(env)
                // Process spark = new SparkLauncher(env)
                .setAppResource(filePath + "/target/lab-hdp-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setMainClass("com.haizhi.graph.hdp.hive.SparkOnHive")
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setSparkHome("/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7")
                .setAppName("Spark On Hive By HDP")
                .setConf(org.apache.spark.launcher.SparkLauncher.DRIVER_MEMORY, "2g")
                .setConf(org.apache.spark.launcher.SparkLauncher.EXECUTOR_CORES, "4")
                .setConf(org.apache.spark.launcher.SparkLauncher.EXECUTOR_MEMORY, "6g")
                .setConf("spark.cores.max", "40")
                .setAppResource("/Users/haizhi/IdeaProjects/lab/lab-hdp/src/main/resource/hbase-site.xml")
                //.setConf("spark.yarn.jars", "hdfs://192.168.1.16:8020/user/graph/lib/spark2x.jars/*")
                //.addAppArgs("{\"outputHBaseTable\":\"to_sim_product\",\"namespace\":\"crm_dev2\",\"tagTable\":\"tag_value\",\"productTable\":\"tv_account\",\"code\":1}")
                .startApplication();

        while (handle.getState() != SparkAppHandle.State.FINISHED) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("applicationId is: " + handle.getAppId());
            System.out.println("current state: " + handle.getState());
//            handle.stop();
        }
    }
}
