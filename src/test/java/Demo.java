import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Person;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;
import utils.ESClient;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Demo {
    private RestHighLevelClient client = ESClient.getClient();
    private String index = "person";
    private String type = "man";

    // 创建批量操作
    @Test
    public void bulkCreateDoc() throws IOException {
        // 准备多个json数据
        Person p1 = new Person(1,"张三",22,new Date());
        Person p2 = new Person(2,"李四",22,new Date());
        Person p3 = new Person(3,"王五",22,new Date());
        // 转为json
        ObjectMapper mapper = new ObjectMapper();
        String json1 = mapper.writeValueAsString(p1);
        String json2 = mapper.writeValueAsString(p2);
        String json3 = mapper.writeValueAsString(p2);
        // request,将数据封装进去
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type,p1.getId().toString()).source(json1,XContentType.JSON));
        request.add(new IndexRequest(index,type,p2.getId().toString()).source(json2,XContentType.JSON));
        request.add(new IndexRequest(index,type,p3.getId().toString()).source(json3,XContentType.JSON));
        // client执行
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

    }

    // 批量删除
    @Test
    public void bulkDeleteDoc() throws IOException {
        BulkRequest request = new BulkRequest();
        // 将要删除的doc的id添加到request
        request.add(new DeleteRequest(index,type,"1"));
        request.add(new DeleteRequest(index,type,"2"));
        request.add(new DeleteRequest(index,type,"3"));
        // client执行
        client.bulk(request,RequestOptions.DEFAULT);

    }

    // 测试连接
    @Test
    public void testConnect(){
        RestHighLevelClient client = ESClient.getClient();
        System.out.println("OK");
    }

    //创建索引
    @Test
    public void createIndex() throws IOException {
        // "number_of_shards": 5,      // 分片数
        //    "number_of_replicas": 1
        // 创建索引
        Settings.Builder settings = Settings.builder()
                .put("number_of_shards", 5)
                .put("number_of_replicas", 1);
        // 准备索引的结构mappings
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("name")
                           .field("type","text")
                        .endObject()
                        .startObject("age")
                           .field("type","integer")
                        .endObject()
                        .startObject("birthday")
                           .field("type","date")
                           .field("format","yyyy-MM-dd")
                        .endObject()
                    .endObject()
                .endObject();
        // 将settings和mappings封装为Request对象
        CreateIndexRequest request = new CreateIndexRequest(index)
                .settings(settings)
                .mapping(type,mappings);
        // 通过Client连接
        CreateIndexResponse res = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(res.toString());
    }


    // 检查索引是否存在
    @Test
    public  void findIndex() throws IOException {
        // 准备request对象
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);

        // 通过client对象操作
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // 输出,
        System.out.println(exists);
    }

    // 删除索引
    @Test
    public  void deleteIndex() throws IOException {
        // 准备request对象
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices(index);

        //通过client对象操作
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);

        // 拿的是否删除成功的结果
        System.out.println(delete.isAcknowledged());
    }


    // 文档创建
    @Test
    public void createDoc() throws IOException {
        // jackson
        ObjectMapper mapper = new ObjectMapper();
        // 1 准备一个json数据
        Person person = new Person(1,"张三",20,new Date());
        String json = mapper.writeValueAsString(person);
        // 2 request对象,手动指定id，使用person对象的id
        IndexRequest request = new IndexRequest(index,type,person.getId().toString());
        request.source(json, XContentType.JSON);//第二个参数告诉他这个参数是json类型
        // 3 通过client操作
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 4 创建成功返回的结果
        String result = response.getResult().toString();
        System.out.println(result); // 成功会返回 CREATED
    }

    // 文档修改
    @Test
    public void updateDoc() throws IOException {
        // 1 创建一个map
        Map<String,Object> doc = new HashMap<>();
        doc.put("name","张三");
        String docId = "1";
        // 2 创建一个request对象，指定要修改哪个，这里指定了index，type和doc的Id,也就是确定唯一的doc
        UpdateRequest request = new UpdateRequest(index, type, docId);
        // 指定修改的内容，也就是上面的map
        request.doc(doc);
        // 3 client对象执行292A2B
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        // 4 执行返回的结果
        String result = update.getResult().toString();
        System.out.println(result);
    }

    // 删除文档
    @Test
    public void deleteDoc() throws IOException {
        // 创建request,指定我要删除1号文档
        DeleteRequest request = new DeleteRequest(index, type, "1");
        // 通过client执行
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        // 获取执行结果
        String result = delete.getResult().toString();
        System.out.println(result); // 返回结果为 DELETED
    }
}
