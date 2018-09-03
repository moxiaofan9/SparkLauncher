package com.haizhi.graph.sparkon;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Created by wangxy on 2018/8/10.
 */


public class ESTransportClient implements Serializable {

    private static TransportClient client;

    public static TransportClient getInstance() {
        return client;
    }

    private ESTransportClient() {
    }

    static  {
        String url = "192.168.1.49,192.168.1.51,192.168.1.52:9300";
        try {
            String[] hosts = StringUtils.substringBefore(url, ":").split(",");
            String port = StringUtils.substringAfter(url, ":");

            Settings settings = Settings.builder().put("cluster.name", "graph").build();

            client = new PreBuiltTransportClient(settings);
            for (String host : hosts) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer
                        .parseInt(port)));
            }

            System.out.println("success to create client of elasticsearch server ");
            if (client.connectedNodes().size() == 0) {
                System.out.println("the ES client doesn't connect to the ES server ");
            }
        } catch (Exception e) {
            System.out.println("failed to create client of elasticsearch server");
        }
    }
}
