
package com.coco.service;

import com.alibaba.fastjson.JSON;
import com.coco.entity.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Description: TODO
 * @author zhangxiaoxun
 * @date 2019/7/21 18:11
 * @Version: V1.0
 *
 **/
@Service
@Slf4j
public class PersonService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private RestClient restClient;

    /**
     * 单个添加
     * @return
     */
    public IndexResponse addPerson() {
        String index = "credential";
        String type = "Person";
        IndexRequest indexRequest = new IndexRequest(index, type);
        Person person = new Person();
        person.setHeight(178);
        person.setName("王力宏");
        person.setCreateTime(System.nanoTime());
        person.setSex(1);
        String source = JSON.toJSONString(person);
        indexRequest.source(source, XContentType.JSON);
        try {
            IndexResponse indexResponse =  restHighLevelClient.index(indexRequest);
            log.info("indexResponse:{}",indexResponse);
            return indexResponse;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 批量添加
     * @return
     */
    public BulkResponse bulkAdd() {
        String index = "credential";
        String type = "Person";
        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest = new IndexRequest(index, type);
        Person person = new Person();
        person.setHeight(178);
        person.setName("孙燕姿");
        person.setCreateTime(System.nanoTime());
        person.setSex(1);
        String source = JSON.toJSONString(person);
        indexRequest.source(source, XContentType.JSON);


        bulkRequest.add(indexRequest);
        bulkRequest.add(indexRequest);

        try {
            BulkResponse bulkResponse =  restHighLevelClient.bulk(bulkRequest);
            log.info("bulkResponse:{}",bulkResponse);
            return bulkResponse;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 根据id删除
     * @return
     */
    public DeleteResponse deletePersonById() {
        String index = "credential";
        String type = "Person";
        String id = "MT1zImwBJAiHfJPnd2no";
        DeleteRequest deleteRequest = new DeleteRequest(index, type,id);
        try {
            DeleteResponse deleteResponse =  restHighLevelClient.delete(deleteRequest);
            log.info("deleteResponse:{}",deleteResponse);
            return deleteResponse;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据条件删除
     * 貌似非常麻烦
     *
     * @return
     */
    public BulkResponse deletePerson() throws IOException {
        String index = "credential";
        String type = "Person";
        String queryCondition = "周杰伦";
        //构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery("content.keyword", queryCondition);
        sourceBuilder.query(termQueryBuilder1);
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        searchRequest.source(sourceBuilder);
        //根据条件查询数据
        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        List<String> docIds = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            docIds.add(hit.getId());
        }

        //循环获取待删除的数据id
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : docIds) {
            DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
            bulkRequest.add(deleteRequest);
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest);
        return bulkResponse;
    }


    /**
     * 拼接restClient实现复杂操作
     *
     * @return
     */
    public Response deletePersonByRestClient() {
        String index = "credential";
        String type = "Person";
        String queryCondition = "周杰伦";
        String endPoint = "/" + index + "/" + type +"/_delete_by_query";
        String source = genereateQueryString(queryCondition);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            return restClient.performRequest("POST", endPoint,Collections.<String, String> emptyMap(),
                    entity);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 查询数据
     * @return
     */
    public SearchResponse search() throws ParseException {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


        String index = "credential";
        String type = "Person";
        //查询只返回name字段
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        //封装条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询返回某个字段
//        sourceBuilder.fetchSource(new String[]{"name"}, new String[]{});
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "周杰伦");
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("createTime");
        rangeQueryBuilder.gte(sdf.parse("2019-07-01 00:00:00").getTime());
        rangeQueryBuilder.lte(sdf.parse("2019-08-01 20:00:00").getTime());
        //复杂查询条件时可以使用boolBuilder
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(matchQueryBuilder);
        boolBuilder.must(rangeQueryBuilder);
        sourceBuilder.query(boolBuilder);
        //将封装的sourceBuilder放入source中
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = restHighLevelClient.search(searchRequest);
            return response;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }

    }



    /**
     * 更新数据
     * @return
     */
    public UpdateResponse update(){
        String index = "credential";
        String type = "Person";
        UpdateRequest updateRequest = new UpdateRequest(index, type, "MD1zImwBJAiHfJPnd2no");
        Map<String, String> map = new HashMap<>();
        map.put("sex", "0");
        updateRequest.doc(map);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return updateResponse;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 封装查询条件
     * @param queryCondition
     * @return
     */
    public String genereateQueryString(String queryCondition){
        IndexRequest indexRequest = new IndexRequest();
        XContentBuilder builder;
        try {
            builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("query")
                    .startObject("term")
                    .field("content.keyword",queryCondition)
                    .endObject()
                    .endObject()
                    .endObject();
            indexRequest.source(builder);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String source = indexRequest.source().utf8ToString();
        return source;
    }




}
