import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;

/**
 * @Auther:LiKang
 * @Date:2021/8/9 -08 -09 -21:16
 * @Description: PACKAGE_NAME
 * @version: 1.0
 */
//对文档的操作
public class ES_document_test {
    @Test//新增文档
    public void document_insert() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        IndexRequest request=new IndexRequest();
        //指定索引（数据库）名称
        request.index("user").id("2001");

        User user=new User("zhangsan","男",20);
        //向ES中插入数据，必须将数据转换为JSON格式
        ObjectMapper mapper=new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        //想请求体中写入要插入的json数据
        request.source(userJson, XContentType.JSON);
        //发送请求，得到响应对象
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
        //关闭ES客户端
        esClient.close();
    }
    @Test//更改文档
    public void document_update() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        UpdateRequest request=new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON,"sex","女");
        //发送请求，得到响应对象
        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
        //关闭ES客户端
        esClient.close();
    }
    @Test//查询文档
    public void document_get() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        GetRequest request=new GetRequest();
        request.index("user").id("1001");
        //发送请求，得到响应对象
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        //关闭ES客户端
        esClient.close();
    }
    @Test//删除文档
    public void document_delete() throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建请求对象
        DeleteRequest request=new DeleteRequest();
        request.index("user").id("1001");
        //发送请求，得到响应对象
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
        //关闭ES客户端
        esClient.close();
    }
}
