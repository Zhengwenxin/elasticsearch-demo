package com.penghaisoft.demo;


import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * @Author Zhengwenxin
 * @Date 2021/11/11 10:13
 * @Version 1.0
 * @Description
 * Elasticsearch压力测试
 */
public class PressureTest {
    


    @Test
    public void test2() throws IOException {

//
//        for (int i = 0; i < 1000; i++) {
//            new Thread(){
//                public void run()  throws InterruptedException{
//
//                }
//            }.start();
//        }


        ElasticsearchDemoTest elasticsearchUtils = new ElasticsearchDemoTest();
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

            elasticsearchUtils.test3(map ,index);
            System.out.println("================Insert Loop:"+i);
        }

    }

}
