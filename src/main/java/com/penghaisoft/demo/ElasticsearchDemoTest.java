package com.penghaisoft.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * @Author Zhengwenxin
 * @Date 2021/11/10 14:06
 * @Version 1.0
 * @Description
 */
@Slf4j
public class ElasticsearchDemoTest{

    public final static String HOST = "127.0.0.1";
    public final static int PORT = 9200;// http请求的端口是9200，客户端是9300

    /**
     * 1.XContentBuilder对象方式 插入数据
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        //1、指定es集群  cluster.name 是固定的key值，my-application是ES集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-application")
               // .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();
        //2.创建访问ES服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT)));

        String index = "test1";
        String type = "_doc";
        // 唯一编号
        String id = "1";
        IndexRequest request = new IndexRequest(index, type, id);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("uid", 1234);
            builder.field("phone", 12345678909L);
            builder.field("msgcode", 1);
            builder.timeField("sendtime", "2021-11-10 15:57:04");
            builder.field("message", "Hello Elasticsearch");
        }
        builder.endObject();
        request.source(builder);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.status());
        //打印出CREATED 表示添加成功
    }

    /**
     * 2.Json字符串方式 插入数据
     * @throws IOException
     */
    @Test
    public void test2() throws IOException {
        //1、指定es集群  cluster.name 是固定的key值，my-application是ES集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-application")
                // .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();
        //2.创建访问ES服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT)));

        String index = "test1";
        String type = "_doc";
        // 唯一编号
        String id = "2";
        IndexRequest request = new IndexRequest(index, type, id);

        String jsonString = "{" + "\"uid\":\"1234\","+ "\"phone\":\"12345678909\","+ "\"msgcode\":\"1\"," + "\"sendtime\":\"2019-03-14 01:57:04\","
                + "\"message\":\"Hello Elasticsearch\"" + "}";
        request.source(jsonString, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.status());
        //打印出CREATED 表示添加成功
    }

    /**
     * 3.Map方式 插入数据
     * @throws IOException
     */
    @Test
    public void test3() throws IOException {
        //1、指定es集群  cluster.name 是固定的key值，my-application是ES集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-application")
                // .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();
        //2.创建访问ES服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT)));

        String index = "test1";
        String type = "_doc";
        // 唯一编号
        String id = "3";
        IndexRequest request = new IndexRequest(index, type, id);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("uid", 1234);
        jsonMap.put("phone", 12345678909L);
        jsonMap.put("msgcode", 1);
        jsonMap.put("sendtime", "2019-03-14 01:57:04");
        jsonMap.put("message", "Hello Elasticsearch");
        request.source(jsonMap);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.status());
        //打印出CREATED 表示添加成功
    }

    /**
     * JDBC方式连接数据库
     * @throws SQLException
     */
    @Test
    public void test4() throws  SQLException {


        String address = "jdbc:es://http://"+HOST+":"+PORT ;
        String sql1 = "SELECT state,COUNT(*) as count,MAX(age) as age ,AVG(balance) as balance FROM account GROUP BY state LIMIT 10";
        String sql2 = "SELECT deviceId,COUNT(*) as count,MAX(temperature) as age ,AVG(voltage) as balance FROM properties_1_2021-11 GROUP BY state,age,balance";

        Properties connectionProperties = connectionProperties();
        Connection connection =
                DriverManager.getConnection(address, connectionProperties);
        long startTime = System.currentTimeMillis();
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(
                     sql2)) {
            while(results.next()){
                System.out.println(results.getString("state")+"    "+
                        results.getString("count")+"    "+
                        results.getString("age")+"    "+
                        results.getString("balance"));
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("总用时："+(endTime-startTime)+"ms");
    }

    private Properties connectionProperties() {
        Properties properties = new Properties();
        properties.put("user", "test_admin");
        properties.put("password", "x-pack-test-password");
        return properties;
    }


    /**
     *
     * @param jsonMap   Map集合
     * @param index 索引名称，索引类型，索引id
     * @throws IOException
     */
    public void test3(HashMap jsonMap,Index index) throws IOException {
        //1、指定es集群  cluster.name 是固定的key值，my-application是ES集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-application")
                // .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();
        //2.创建访问ES服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT)));

        IndexRequest request = new IndexRequest(index.getIndex(), index.getType(), index.getId());
        request.source(jsonMap);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.status());
        client.close();
        //打印出CREATED 表示添加成功
    }

    @Test
    public void test5() throws IOException {
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
