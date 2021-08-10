import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;

/**
 * @Auther:LiKang
 * @Date:2021/8/9 -08 -09 -21:57
 * @Description: PACKAGE_NAME
 * @version: 1.0
 */
//批量操作文档
public class ES_document_batch_test {
    @Test//批量新增
    public void document_batch_insert() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //批量请求对象，可存放多个请求对象
        BulkRequest request=new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "张三","age",20,"sex","男");
        IndexRequest indexRequest2 = new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "李四","age",30,"sex","女");
        IndexRequest indexRequest3 = new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "王五","age",40,"sex","女");
        IndexRequest indexRequest4 = new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "王五1","age",21,"sex","男");
        IndexRequest indexRequest5 = new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "王五2","age",26,"sex","男");
        IndexRequest indexRequest6 = new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "王五3","age",50,"sex","男");
        IndexRequest indexRequest7 = new IndexRequest().index("user").id("1007").source(XContentType.JSON, "name", "王五4","age",10,"sex","男");
        IndexRequest indexRequest8 = new IndexRequest().index("user").id("1008").source(XContentType.JSON, "name", "王五555","age",29,"sex","男");


        request.add(indexRequest1);
        request.add(indexRequest2);
        request.add(indexRequest3);
        request.add(indexRequest4);
        request.add(indexRequest5);
        request.add(indexRequest6);
        request.add(indexRequest7);
        request.add(indexRequest8);
        //发送请求，得到响应对象
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());
        System.out.println(response.getItems());
        //关闭ES客户端
        esClient.close();
    }
    @Test
    public void document_batch_delete() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //批量请求对象，可存放多个请求对象
        BulkRequest request=new BulkRequest();
        DeleteRequest deleteRequest1=new DeleteRequest().index("user").id("1001");
        DeleteRequest deleteRequest2=new DeleteRequest().index("user").id("1002");
        DeleteRequest deleteRequest3=new DeleteRequest().index("user").id("1003");

        request.add(deleteRequest1);
        request.add(deleteRequest2);
        request.add(deleteRequest3);

        //发送请求，得到响应对象
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());
        System.out.println(response.getItems());
        //关闭ES客户端
        esClient.close();
    }

}
