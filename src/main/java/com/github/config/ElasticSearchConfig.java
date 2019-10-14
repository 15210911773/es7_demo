package com.github.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class ElasticSearchConfig {

    private static final int CONNECT_TIMEOUT = 1000;
    private static final int SOCKET_TIMEOUT = 30000;
    private static final int CONNECTION_REQUEST_TIMEOUT = 500;

    @Value("${elasticsearch.username}")
    private  String username;
    @Value("${elasticsearch.password}")
    private  String password;
    @Value("${elasticsearch.cluster-nodes}")
    private  String clusterNodes;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient(){
        final CredentialsProvider credentialsProvider=new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
        HttpHost[] hostArray = getHttpHostArray();
        RestClientBuilder builder= RestClient.builder(hostArray);
        //set config callback
        builder.setHttpClientConfigCallback(httpClientBuilder->httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider) );
        //config async timeout
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                builder.setConnectTimeout(CONNECT_TIMEOUT);
                builder.setSocketTimeout(SOCKET_TIMEOUT);
                builder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT);
                return builder;
            }
        });
        //config asyn connect num
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return  client;
    }

    private HttpHost[] getHttpHostArray() {
        String[] hostAndPort = clusterNodes.split(",");
        int index = 0;
        List<HttpHost> list = new ArrayList<>();
        for (String s : hostAndPort) {
            String[] arr = s.split(":");
            if (arr.length < 2) {
                continue;
            }
            String ip = arr[0].trim();
            String port = arr[1].trim();
            HttpHost httpHost = new HttpHost(ip, Integer.valueOf(port), "http");
            list.add(httpHost);
        }
        HttpHost[] httpHosts = new HttpHost[list.size()];
        for (int i = 0; i < list.size(); i++) {
            httpHosts[i] = list.get(i);
        }
        return httpHosts;
    }


}
