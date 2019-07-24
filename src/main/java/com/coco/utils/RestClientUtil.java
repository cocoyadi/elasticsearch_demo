
package com.coco.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Description: 低阶客户端（TransportClient将会在Elasticsearch 7.0弃用并在8.0中完成删除，替而代之）
 * @author zhangxiaoxun
 * @date 2019/7/21 13:05
 * @Version: V1.0
 *
 **/
public class RestClientUtil {
    /**
 * 高阶Rest Client
 */
private RestClient restClient = null;


    public RestClientUtil() {
        if (restClient == null) {
            synchronized (RestHighLevelClient.class) {
                if (restClient == null) {
                    restClient = createRestClient();
                }
            }
        }
    }

    /**
     * 创建低阶客户端
     * @return
     */
    private RestClient createRestClient() {
        RestClient client = null;

        try {
            client = RestClient.builder(
                            new HttpHost("10.51.2.9", 9200, "http")
                    ).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    public void closeClient() {
        try {
            if (restClient != null) {
                restClient.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
