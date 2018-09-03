package com.haizhi.graph.sparklauncher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.net.URL;
import java.util.HashMap;

/**
 * Created by wangxy on 2018/3/9.
 */

/**
 * 第一个参数为HADOOP_CONF_DIR的配置路径，即将resouces下的配置文件拷贝到jar包外的一个文件夹下.
 */
public class SparkLauncherTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("SPARK_HOME", "/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7");

        URL url = SparkLauncherTest.class.getProtectionDomain().getCodeSource().getLocation();
        String path = url.getPath();
        String filePath = path.substring(0, path.lastIndexOf("/") + 1);
        System.out.println("filePath=" + filePath);

        filePath = "/Users/haizhi/IdeaProjects/graph-platform/build/release/graph/graph-alg-2.0.1-SNAPSHOT/lib";
        //"/Users/haizhi/IdeaProjects/SparkLauncher/target/";

        HashMap<String, String> env = new HashMap<String, String>(2);
        //提交服务器路径地址
        env.put("HADOOP_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");
        env.put("YARN_CONF_DIR", "/Users/haizhi/IdeaProjects/SparkLauncher/src/main/resources");

        SparkAppHandle handle = new SparkLauncher(env)
                // Process spark = new SparkLauncher(env)
                .setAppResource(filePath + "/graph-alg-recommend-driver.jar")
                .setMainClass("com.haizhi.graph.alg.recommend.RecommendDriver") // com.haizhi.graph.sparklauncher.SparkLauncherMain
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setSparkHome("/Users/haizhi/IDE/spark-2.1.0-bin-hadoop2.7")
                .setAppName("SparkLauncherTest")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "4")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "6g")
                .setConf("spark.executor.instances", "10")
                .setConf("spark.yarn.jars", "hdfs://192.168.1.16:8020/user/graph/lib/spark2x.jars/*")
                .addAppArgs("{\"outputHBaseTable\":\"to_sim_product\",\"namespace\":\"crm_dev2\",\"tagTable\":\"tag_value\",\"productTable\":\"tv_account\",\"code\":1}")
                .startApplication();

     /*   int exitCode = spark.waitFor();
        System.out.println("exitCode " + exitCode);*/
        // Use handle API to monitor / control application.
        /*handle.getAppId();
        handle.getState();
        handle.addListener(new MySparkListener());*/

        /*handle.addListener(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {

            }

            @Override
            public void infoChanged(SparkAppHandle handle) {

            }
        });*/

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
