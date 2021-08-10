
import org.apache.http.HttpHost;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;

/**
 * @Auther:LiKang
 * @Date:2021/8/10 -08 -10 -7:35
 * @Description: PACKAGE_NAME
 * @version: 1.0
 */
//对文档的高级查询操作
public class ES_document_query {


    @Test//全量查询
    public void fullQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }

    @Test//条件查询
    public void conditionQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.termQuery("age",30));//查询age=30的数据

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }

    @Test//分页查询
    public void pagingQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());//全量查询
        builder.from(0);//分页的起始位置
        builder.size(2);//每页的条数
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//字段排序+分页查询
    public void fieldSortingAndPagingQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());//全量查询
        builder.sort("age", SortOrder.DESC);//按照年龄降序
        builder.from(0);//分页的起始位置
        builder.size(5);//每页的条数
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//过滤字段
    public void filterField() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());//全量查询
        String[] excludes={};
        String[] includes={"age","name"};
        builder.fetchSource(includes,excludes);//指定包含字段和排除字段
        builder.fetchSource();
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//组合查询(多条件查询)
    public void combinationConditionQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();

        //boolQueryBuilder.must(QueryBuilders.matchQuery("age", 30));//年龄必须是30
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 30));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 40));//年龄30或者40
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex", "男"));
        builder.query(boolQueryBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//范围查询
    public void rangeQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");

        rangeQuery.gte(30);//大于等于30
        rangeQuery.lte(40);//小于等于40
        // rangeQuery.gt(30);//大于30
        //rangeQuery.lt(40);//小于40
        builder.query(rangeQuery);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//模糊查询
    public void fuzzyQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        FuzzyQueryBuilder fuzzyBuilder = QueryBuilders.fuzzyQuery("name", "王五").fuzziness(Fuzziness.TWO);//Fuzziness.ONE表示相差一个字符；Fuzziness.TWO表示相差两个字符


        builder.query(fuzzyBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//高亮查询
    public void highlightQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "zhangsan");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color:'red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");

        builder.highlighter(highlightBuilder );
        builder.query(termsQueryBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getHighlightFields());
        }

        //关闭ES客户端
        esClient.close();
    }
    @Test//最大值查询
    public void maximumQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder= AggregationBuilders.max("maxAge").field("age");//求年龄的最大值

        builder.aggregation(aggregationBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getHighlightFields());
        }
        //关闭ES客户端
        esClient.close();
    }
    @Test//分组查询(聚合查询)
    public void groupQuery() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        SearchRequest request=new SearchRequest();
        request.indices("user");
        //高级查询对象
        SearchSourceBuilder builder=new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder=AggregationBuilders.terms("ageGroup").field("age");
        builder.aggregation(aggregationBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询到的条数
        System.out.println(response.getTook());

        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }
        //关闭ES客户端
        esClient.close();
    }

}
