import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.Test;

import java.io.IOException;

/**
 * @Auther:LiKang
 * @Date:2021/8/9 -08 -09 -18:28
 * @Description: PACKAGE_NAME
 * @version: 1.0
 */
//对索引的操作
public class ES_test {
    @Test
    public void ESTest_Client() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));

        //关闭ES客户端
        esClient.close();
    }

    @Test//索引的创建
    public void ESTest_index_Create() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient= new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest("user");
        CreateIndexResponse createIndexResponse=esClient.indices().create(request, RequestOptions.DEFAULT);
        //响应状态
        boolean acknowledged=createIndexResponse.isAcknowledged();
        System.out.println("索引操作："+acknowledged);
        //关闭ES客户端
        esClient.close();


    }
    @Test//查询索引
    public void ESTest_index_Search() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient= new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        //查询索引
        GetIndexRequest request = new GetIndexRequest("user");

        GetIndexResponse getIndexResponse=esClient.indices().get(request,RequestOptions.DEFAULT);
        //响应状态
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
        //关闭ES客户端
        esClient.close();
    }
    @Test
    public void ESTest_index_Delete() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient= new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        //删除索引
        DeleteIndexRequest request = new DeleteIndexRequest("user");

        AcknowledgedResponse response = esClient.indices().delete(request, RequestOptions.DEFAULT);
        //响应状态
        System.out.println(response.isAcknowledged());
        //关闭ES客户端
        esClient.close();
    }
}
