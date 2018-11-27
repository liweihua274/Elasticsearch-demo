package io.ymq.example.elasticsearch.service;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Date;

/**
 * Created by liweihua on 2018/11/26 0026.
 */

@RestController
@RequestMapping("/demo01")
public class ElasticTest {

    @Autowired
    private TransportClient transportClient;

    @RequestMapping("/batchInsert")
    public String hello1() {
        long start = System.currentTimeMillis();
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        try {
            for (int i = 6000000; i < 6500000; i++) {
                bulkRequest.add(transportClient.prepareIndex("adang1", "test1", String.valueOf(i))
                        .setSource(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("user", "lzq")
                                .field("postDate", new Date())
                                .field("message", "trying out Elasticsearch")
                                .field("sendDate", "2018-11-1" + String.valueOf(i % 8))
                                .field("msg", "你好李四")
                                .field("account", 1 + i)
                                .field("memberId", "13636993622121" + String.valueOf(i % 8))
                                .field("mobileNo", "13636993695")
                                .field("billno", "b" + i)
                                .endObject()
                        )
                );
                System.out.println("当前位置："+ i);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            System.out.println("failures..............:" + bulkResponse.buildFailureMessage()
            );
        }
        long res = System.currentTimeMillis();
        System.out.println("【会员异常积分】统计结束，耗时 "+ String.valueOf(res - start)+" 毫秒");
        System.out.println("结束");
        return "hello";
    }



    @RequestMapping("/search")
    public String search() {
        long start = System.currentTimeMillis();
        GetResponse getResponse = transportClient.prepareGet("adang1", "test1", "300000").get();
        System.out.println("索引库的数据:" + getResponse.getSourceAsString());
        long res = System.currentTimeMillis();
        System.out.println("【会员异常积分】统计结束，耗时 "+ String.valueOf(res - start)+" 毫秒");
        return "";
    }

    @RequestMapping("/searchBatch")
    public String searchBatch() {
        long start = System.currentTimeMillis();
       // RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("billno").gte("b109947");

        QueryBuilder qb=QueryBuilders.boolQuery().must(QueryBuilders.termQuery("memberId","136369936221212")).must(QueryBuilders.termQuery("sendDate","2018-11-12")) ;

        //QueryBuilder s = QueryBuilders.boolQuery().must(rangeQueryBuilder);//.must(qb5);
        SearchRequestBuilder sv = transportClient.prepareSearch("adang1").setTypes("test1").setQuery(qb).setFrom(0)
                .setSize(100);
        System.out.println(sv.toString());
        SearchResponse response = sv.get();
        SearchHits searchHits = response.getHits();
        for (SearchHit hit : searchHits.getHits()) {
            System.out.println(hit.getSourceAsString());

        }
        long res = System.currentTimeMillis();
        System.out.println("【会员异常积分】统计结束，耗时 "+ String.valueOf(res - start)+" 毫秒");
        return "finish";
    }


    @RequestMapping("/searchBatch1")
    //统计范围
    public String searchBatch1() {
        long start = System.currentTimeMillis();
        AggregationBuilder termsBuilder = AggregationBuilders.count("ageCount").field("account");

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("account").from(5083,true).to(6000,true);
        QueryBuilder s=QueryBuilders.boolQuery().must(rangeQueryBuilder);//.must(qb5);
        SearchRequestBuilder sv=transportClient.prepareSearch("adang1").setTypes("test1").setQuery(s).setFrom(0).setSize(1000).addAggregation(termsBuilder);
        System.out.println(sv.toString());
        SearchResponse response=  sv.get();
        SearchHits searchHits =  response.getHits();
        for(SearchHit hit:searchHits.getHits()){
            System.out.println(hit.getSourceAsString());
        }
        ValueCount valueCount= response.getAggregations().get("ageCount");
        long value=valueCount.getValue();
        long res = System.currentTimeMillis();
        System.out.println("【会员异常积分】统计结束，耗时 "+ String.valueOf(res - start)+" 毫秒");
        return String.valueOf(value);

    }


    @RequestMapping("/searchBatchGroup")
    //分组统计
    public String searchBatchGroup() {
        long start = System.currentTimeMillis();
        AggregationBuilder  termsBuilder = AggregationBuilders.terms("by_sendDate").field("sendDate");
        AggregationBuilder  sumBuilder=AggregationBuilders.sum("accountSum").field("account");
        AggregationBuilder  avgBuilder=AggregationBuilders.avg("accountAvg").field("account");
        AggregationBuilder  countBuilder=AggregationBuilders.count("accountCount").field("account");

        termsBuilder.subAggregation(sumBuilder).subAggregation(avgBuilder).subAggregation(countBuilder);
        //TermsAggregationBuilder all = AggregationBuilders.terms("age").field("age");
        //all.subAggregation(termsBuilder);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("sendDate").from("2018-11-10",true).to("2018-11-13",true);
        QueryBuilder s=QueryBuilders.boolQuery().must(rangeQueryBuilder);//.must(QueryBuilders.termQuery("memberId","136369936221212"));
        SearchRequestBuilder sv=transportClient.prepareSearch("adang1").setTypes("test1").setQuery(s).setFetchSource(null,"sendDate").setFrom(0).setSize(10000).addAggregation(termsBuilder);
        System.out.println(sv.toString());
        SearchResponse response=  sv.get();

        Aggregations terms= response.getAggregations();
        for (Aggregation a:terms){
            LongTerms teamSum= (LongTerms)a;
            for(LongTerms.Bucket bucket:teamSum.getBuckets()){
                System.out.println((bucket.getKeyAsString()+"   "+bucket.getDocCount()+"    "+
                        ((Sum)bucket.getAggregations().asMap().get("accountSum")).getValue()+"    "+
                        ((Avg)bucket.getAggregations().asMap().get("accountAvg")).getValue()+"    "+
                        ((ValueCount)bucket.getAggregations().asMap().get("accountCount")).getValue()));

            }
        }
        long res = System.currentTimeMillis();
        System.out.println("【会员异常积分】统计结束，耗时 "+ String.valueOf(res - start)+" 毫秒");
        return "finish";

    }



}
