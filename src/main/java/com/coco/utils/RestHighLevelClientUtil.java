
package com.coco.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Description: 访问elastic客户端工具类
 * @author zhangxiaoxun
 * @date 2019/7/21 12:57
 * @Version: V1.0
 *
 **/
public class RestHighLevelClientUtil {
    /**
     * 高阶Rest Client
     */
    private RestHighLevelClient highLevelClient = null;


    public RestHighLevelClientUtil() {
        if (highLevelClient == null) {
            synchronized (RestHighLevelClient.class) {
                if (highLevelClient == null) {
                    highLevelClient = createHighLevelClient();
                }
            }
        }
    }

    /**
     * 创建高阶客户端
     * @return
     */
    private RestHighLevelClient createHighLevelClient() {
        RestHighLevelClient client = null;

        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("10.51.2.9", 9200, "http")
                    )
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }


    /**
     * 关闭客户端
     */
    public void closeClient() {
        try {
            if (highLevelClient != null) {
                highLevelClient.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
