package com.haizhi.graph.sparklauncher;

import org.apache.spark.launcher.*;

import java.util.HashMap;

/**
 * Created by wangxy on 2018/8/7.
 */


public class SparkOnEsLauncher {
    public static void main(String[] args) throws Exception {
        // System.setProperty("SPARK_HOME", "/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7");

        String filePath = "/Users/haizhi/IdeaProjects/SparkLauncher/target";

        HashMap<String, String> env = new HashMap<>(2);
        //提交服务器路径地址
        env.put("HADOOP_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");
        env.put("YARN_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");

        SparkAppHandle handle = new org.apache.spark.launcher.SparkLauncher(env)
                // Process spark = new SparkLauncher(env)
                .setAppResource(filePath + "/SparkLauncher-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setMainClass("com.haizhi.graph.sparkon.SparkOnEsForJava")
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setSparkHome("/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7")
                .setAppName("SparkLauncherTest")
                .setConf(org.apache.spark.launcher.SparkLauncher.DRIVER_MEMORY, "2g")
                .setConf(org.apache.spark.launcher.SparkLauncher.EXECUTOR_CORES, "4")
                .setConf(org.apache.spark.launcher.SparkLauncher.EXECUTOR_MEMORY, "6g")
                .setConf("spark.executor.instances", "10")
                .setConf("spark.yarn.jars", "hdfs://192.168.1.16:8020/user/graph/lib/spark2x.jars/*")
                .addAppArgs("/user/wangxy/Person", "40")
                .startApplication();

        while (handle.getState() != SparkAppHandle.State.FINISHED) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("applicationId is: " + handle.getAppId() + ",and current state: " + handle.getState());
        }
    }
}
