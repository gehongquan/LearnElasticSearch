package sms;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import utils.ESClient;
import java.io.IOException;
import java.util.Map;

// 各种查询
public class QueryTest {
    private final String index = "sms-logs-index";
    private final String type = "sms-logs-type";
    // client对象
    private final RestHighLevelClient client = ESClient.getClient();

    // term查询
    @Test
    public void termQuery() throws IOException {
        //1  request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //2 指定查询条件
            // 指定form ,size
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(5);
             // 指定查询条件,province字段，内容为北京
        builder.query(QueryBuilders.termQuery("province","上海"));
        request.source(builder);
        //3执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4 获取到数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }

    // terms查询
    @Test
    public void termsQuery() throws IOException {
        // request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termsQuery("province","上海","河南"));
        request.source(builder);
        // 执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }

    // match_all查询
    @Test
    public void matchAllQuery() throws IOException {
        // request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        // builder.size(20); 在这里指定要显示的个数，es默认只回显10条
        request.source(builder);
        // 执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }

    // match查询
    @Test
    public void matchQuery() throws IOException {
        // request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent","电话号码"));
        request.source(builder);
        // 执行查询
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        // 获取数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }

    // 布尔match查询
    @Test
    public void booleanMatchQuery() throws IOException {
        // request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent","电话 快递").operator(Operator.AND));
        request.source(builder);
        // 执行查询
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        // 获取数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }


    // multi_match查询
    @Test
    public void multiMatchQuery() throws IOException {
        // request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.multiMatchQuery("中国","smsContent","province"));
        request.source(builder);
        // 执行查询
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        // 获取数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> result = hit.getSourceAsMap();
            System.out.println(result);
        }
    }

    // id查询
    @Test
    public void idQuery() throws IOException {
        // 使用getRequest
        GetRequest request = new GetRequest(index,type,"1");
        // 执行查询
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 输出结果
        Map<String, Object> result = response.getSourceAsMap();
        System.out.println(result);
    }

    // ids查询
    @Test
    public void idsQuery() throws IOException {
        // 这个属于复杂查询需要使用searchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.idsQuery().addIds("1","2","3"));

        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 输出结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // prefix查询
    @Test
    public void prefixQuery() throws IOException {
        // 依然使用SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.prefixQuery("corpName","滴滴"));
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // fuzzy查询
    @Test
    public void fuzzyQuery() throws IOException {
        // 依然使用SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("corpName","中国移不动"));
        //builder.query(QueryBuilders.fuzzyQuery("corpName","中国移不动").prefixLength(2));
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // wildcard查询
    @Test
    public void wildcardQuery() throws IOException {
        // 依然使用SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.wildcardQuery("corpName","中国??"));
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // range查询
    @Test
    public void rangeQuery() throws IOException {
        // 依然使用SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("fee").gte(5).lte(10));
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
    // regexp查询
    @Test
    public void regexpQuery() throws IOException {
        // 依然使用SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 查询
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.regexpQuery("mobile","15[0-9]{8}"));
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // scroll查询
    @Test
    public void scrollQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 指定scroll的信息，存在内存1分钟
        request.scroll(TimeValue.timeValueMinutes(1L));
        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(1);
        builder.sort("fee", SortOrder.ASC);
        builder.query(QueryBuilders.matchAllQuery());
        // 把SearchSourceBuilder放到Request中，千万别忘了
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取第一页的结果结果，以及scrollId
        String scrollId = response.getScrollId();
        System.out.println("------第一页------");
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }

        // 循环遍历其余页
        while (true){
            // SearchScrollRequest,指定生存时间,scrollId
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            // 执行查询
            SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);

            // 如果查询到了数据
            SearchHit[] hits = scrollResponse.getHits().getHits();
            if (hits !=null && hits.length>0){
                System.out.println("------下一页------");
                for (SearchHit hit : hits) {
                    System.out.println(hit.getSourceAsMap());
                }
            }else {
                System.out.println("-----最后一页-----");
                break;
            }
        }
        // ClearScrollRequest
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        // 删除ScoreId
        ClearScrollResponse scrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        System.out.println("删除scroll成功了吗？"+scrollResponse.isSucceeded());
    }

    // deleteByQuery查询
    @Test
    public void deleteByQuery() throws IOException {
        // DeleteByQueryRequest
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.types(type);

        // 指定检索条件
        request.setQuery(QueryBuilders.rangeQuery("fee").lt(4));

        // 执行删除
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);

        System.out.println(response);
    }

    // boolQuery查询
    @Test
    public void boolQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 上海或者河南
        boolQuery.should(QueryBuilders.termQuery("province","武汉"));
        boolQuery.should(QueryBuilders.termQuery("province","河南"));
        // 运营商不是联通
        boolQuery.mustNot(QueryBuilders.termQuery("operatorId",2));
        // 包含中国和移动
        boolQuery.must(QueryBuilders.matchQuery("smsContent","中国"));
        boolQuery.must(QueryBuilders.matchQuery("smsContent","移动"));
        // 指定使用bool查询
        builder.query(boolQuery);
        request.source(builder);

        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // boostingQuery查询
    @Test
    public void boostingQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoostingQueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery(
                QueryBuilders.matchQuery("smsContent", "亲爱的"),
                QueryBuilders.matchQuery("smsContent", "网易")
        ).negativeBoost(0.5f);
        builder.query(boostingQueryBuilder);
        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // filterQuery查询
    @Test
    public void filterQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.filter(QueryBuilders.termQuery("corpName","中国移动"));
        boolQuery.filter(QueryBuilders.rangeQuery("fee").lte(5));

        builder.query(boolQuery);
        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    // highlightQuery查询
    @Test
    public void highlightQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);

        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent", "亲爱的"));
        // 高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("smsContent",10) // 只显示10个字
        .preTags("<font color='read'>").postTags("</font>");    // 红色展示

        builder.highlighter(highlightBuilder);
        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果,拿高亮的内容
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getHighlightFields());
        }
    }

    // 去重记数查询
    @Test
    public void cardinalityQuery() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);

        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.cardinality("agg").field("province"));
        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果,拿到总数,因为Aggregation是一个接口，我们需要向下转型，使用实现类的方法才能拿的value
        Cardinality agg = response.getAggregations().get("agg");
        long value = agg.getValue();
        System.out.println("省份总数为："+value);

        // 拿到查询的内容
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }


    // 范围统计查询
    @Test
    public void range() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);

        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.range("agg").field("fee")
                .addUnboundedTo(5)   // 指定范围
                .addRange(5,10)
                .addUnboundedFrom(10));

        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果
        Range agg = response.getAggregations().get("agg");
        for (Range.Bucket bucket : agg.getBuckets()) {
            String key = bucket.getKeyAsString();
            Object from = bucket.getFrom();
            Object to = bucket.getTo();
            long docCount = bucket.getDocCount();
            System.out.println(String.format("key：%s，from：%s，to：%s，docCount：%s",key,from,to,docCount));
        }
    }

    // 聚合查询
    @Test
    public void extendedStats() throws IOException {
        // SearchRequest
        SearchRequest request = new SearchRequest(index);
        request.types(type);

        // 指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.extendedStats("agg").field("fee"));

        request.source(builder);
        // client执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 获取结果
        ExtendedStats agg = response.getAggregations().get("agg");
        double max = agg.getMax();
        double min = agg.getMin();

        System.out.println("fee的最大值为"+max);
        System.out.println("fee的最小值为"+min);
    }
}
