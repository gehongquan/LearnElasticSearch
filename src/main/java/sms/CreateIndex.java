package sms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Person;
import entity.SmsLogs;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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

// 创建索引，并指定doc的测试数据
public class CreateIndex {
    private String index = "sms-logs-index";
    private String type = "sms-logs-type";
    // client对象
    private RestHighLevelClient client = ESClient.getClient();

    // 创建索引
    @Test
    public void CreateIndexForSms() throws IOException {
        // 创建索引
        Settings.Builder settings = Settings.builder()
                .put("number_of_shards", 5)
                .put("number_of_replicas", 1);
        // 指定mappings
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("createDate")
                            .field("type", "date")
                            .field("format","yyyy-MM-dd")
                        .endObject()
                        .startObject("sendDate")
                            .field("type", "date")
                            .field("format", "yyyy-MM-dd")
                        .endObject()
                            .startObject("longCode")
                            .field("type", "keyword")
                        .endObject()
                            .startObject("mobile")
                            .field("type", "keyword")
                        .endObject()
                            .startObject("corpName")
                            .field("type", "keyword")
                        .endObject()
                            .startObject("smsContent")
                            .field("type", "text")
                            .field("analyzer", "ik_max_word")
                        .endObject()
                            .startObject("state")
                            .field("type", "integer")
                        .endObject()
                            .startObject("operatorId")
                            .field("type", "integer")
                        .endObject()
                            .startObject("province")
                            .field("type", "keyword")
                        .endObject()
                            .startObject("ipAddr")
                            .field("type", "ip")
                        .endObject()
                            .startObject("replyTotal")
                            .field("type", "integer")
                        .endObject()
                            .startObject("fee")
                            .field("type", "long")
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
    // 测试数据
    @Test
    public void CreateTestData() throws IOException {
        // 准备多个json数据
        SmsLogs s1 = new SmsLogs("1",new Date(),new Date(),"10690000988","1370000001","途虎养车","【途虎养车】亲爱的刘女士，您在途虎购买的货物单号(Th12345678)",0,1,"上海","10.126.2.9",10,3);
        SmsLogs s2 = new SmsLogs("2",new Date(),new Date(),"84690110988","1570880001","韵达快递","【韵达快递】您的订单已配送不要走开哦,很快就会到了,配送员:王五，电话:15300000001",0,1,"上海","10.126.2.8",13,5);
        SmsLogs s3 = new SmsLogs("3",new Date(),new Date(),"10698880988","1593570001","滴滴打车","【滴滴打车】指定的车辆现在距离您1000米,马上就要到了,请耐心等待哦,司机:李师傅，电话:13890024793",0,1,"河南","10.126.2.7",12,10);
        SmsLogs s4 = new SmsLogs("4",new Date(),new Date(),"20697000911","1586890005","中国移动","【中国移动】尊敬的客户，您充值的话费100元，现已经成功到账，您的当前余额为125元,2020年12月18日14:35",0,1,"北京","10.126.2.6",11,4);
        SmsLogs s5 = new SmsLogs("5",new Date(),new Date(),"18838880279","1562384869","网易","【网易】亲爱的玩家,您已经排队成功,请尽快登录到网易云游戏进行游玩,祝您游戏愉快---网易云游戏",0,1,"杭州","10.126.2.5",10,2);
        // 转为json
        ObjectMapper mapper = new ObjectMapper();
        String json1 = mapper.writeValueAsString(s1);
        String json2 = mapper.writeValueAsString(s2);
        String json3 = mapper.writeValueAsString(s3);
        String json4 = mapper.writeValueAsString(s4);
        String json5 = mapper.writeValueAsString(s5);

        // request,将数据封装进去
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type,s1.getId()).source(json1,XContentType.JSON));
        request.add(new IndexRequest(index,type,s2.getId()).source(json2,XContentType.JSON));
        request.add(new IndexRequest(index,type,s3.getId()).source(json3,XContentType.JSON));
        request.add(new IndexRequest(index,type,s4.getId()).source(json4,XContentType.JSON));
        request.add(new IndexRequest(index,type,s5.getId()).source(json5,XContentType.JSON));
        // client执行
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }
}
