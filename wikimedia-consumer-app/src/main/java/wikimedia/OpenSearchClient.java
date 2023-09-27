package wikimedia;

import java.net.URI;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchClient {
        private static final Logger log = LoggerFactory.getLogger(OpenSearchClient.class.getName());
        public static RestHighLevelClient createOpenSearchClient() {
            String connString = "http://localhost:9200";
    //        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";
    
            // we build a URI from the connection string
            RestHighLevelClient restHighLevelClient;
            URI connUri = URI.create(connString);
            // extract login information if it exists
            String userInfo = connUri.getUserInfo();
    
            if (userInfo == null) {
                // REST client without security
                restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
    
            } else {
                // REST client with security
                String[] auth = userInfo.split(":");
    
                CredentialsProvider cp = new BasicCredentialsProvider();
                cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
                log.info(String.format("The credentials are username %s and password is %s",auth[0],auth[1]));
                restHighLevelClient = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                                .setHttpClientConfigCallback(
                                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    
    
            }
    
            return restHighLevelClient;
        }
}
