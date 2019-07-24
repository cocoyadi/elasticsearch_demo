
package com.coco.config;

import com.coco.client.ESClientSpringFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

/**
 * @Description: TODO
 * @author zhangxiaoxun
 * @date 2019/7/21 18:00
 * @Version: V1.0
 *
 **/
@Configuration
@PropertySource("classpath:elasticsearch.properties")
public class ESConfig {
    @Value("${httpHost.host}")
    private String host;

    @Value("${httpHost.port}")
    private int port;

    @Value("${httpHost.schema}")
    private String schema;

    @Value("${esclient.connectNum}")
    private Integer connectNum;

    @Value("${esclient.connectPerRoute}")
    private Integer connectPerRoute;



    @Bean
    public HttpHost httpHost(){
        return new HttpHost(host,port,schema);
    }

    @Bean(initMethod="init",destroyMethod="close")
    public ESClientSpringFactory getFactory(){
        return ESClientSpringFactory.
                build(httpHost(), connectNum, connectPerRoute);
    }

    @Bean
    @Scope("singleton")
    public RestClient getRestClient(){
        return getFactory().getClient();
    }

    @Bean
    @Scope("singleton")
    public RestHighLevelClient getRHLClient(){
        return getFactory().getRhlClient();
    }
}
