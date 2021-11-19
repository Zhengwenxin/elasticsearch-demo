package com.penghaisoft.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * @Author Zhengwenxin
 * @Date 2021/11/15 11:10
 * @Version 1.0
 * @Description
 */

public class TestThread implements Runnable{
    public final static String HOST = "127.0.0.1";
    public final static int PORT = 9200;// http请求的端口是9200，客户端是9300

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            HashMap map = new HashMap();
            Long idLong = UUID.randomUUID().getMostSignificantBits()& Long.MAX_VALUE;
            map.put("id",idLong.toString());
            map.put("name",idLong.toString());
            map.put("time",new Date());
            map.put("value",idLong);
            Long id = UUID.randomUUID().getMostSignificantBits()& Long.MAX_VALUE;
            Index index=  Index.builder()
                    .index("test1")
                    .id(id.toString())
                    .type("_doc")
                    .build();

//                this.test3(map ,index);
            Settings settings = Settings.builder()
                    .put("cluster.name", "my-application")
                    // .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                    .build();
            //2.创建访问ES服务器的客户端
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT)));

            IndexRequest request = new IndexRequest(index.getIndex(), index.getType(), index.getId());
            request.source(map);
            IndexResponse response = null;
            try {
                response = client.index(request, RequestOptions.DEFAULT);
                System.out.println(response.status());
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


            System.out.println("================Insert Loop:"+(i+1));
        }
        long endTime = System.currentTimeMillis();
        System.out.println("总用时："+(endTime-startTime)+"ms");
    }
}
