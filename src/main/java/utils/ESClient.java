package utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

// es连接对象
public class ESClient {
    public static RestHighLevelClient  getClient(){
        // 指定es服务器的ip,端口
        HttpHost httpHost = new HttpHost("192.168.10.106",9200);
        RestClientBuilder builder = RestClient.builder(httpHost);
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
