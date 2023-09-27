package wikimedia;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.lang.String.format;
import wikimedia.kafka.KafkaConfig;

public class OpenSearchConsumer {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
    //private static RestHighLevelClient createOpenSearchClient;
    private static final String GROUP_ID = "opensearch-consumer-group";
    private static final String OFFSET_CONFIG = "latest";
    private static final String TOPIC = "wikimedia.change.stream";
    private static String RESOURCE_LOCATION = "wikimedia";
    public static void main(String[] args) throws IOException {
        var openSearchClient = OpenSearchClient.createOpenSearchClient();
        
        try(openSearchClient) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(RESOURCE_LOCATION), RequestOptions.DEFAULT);
            if(!indexExists){
            var req = new CreateIndexRequest(RESOURCE_LOCATION);
            openSearchClient.indices().create(req, RequestOptions.DEFAULT);
            log.info(format("The new index %s is created",RESOURCE_LOCATION));
            }else{log.info(format("The given index %s already exists",RESOURCE_LOCATION));}

            KafkaConsumer<String,String> consumer = new KafkaConsumer<>(
                KafkaConfig.getLocalServerConsumerProperties(GROUP_ID,OFFSET_CONFIG,
                StringDeserializer.class.getName(), StringDeserializer.class.getName()));

            consumer.subscribe(Collections.singletonList(TOPIC));

            while(true){
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(3000));
                log.info("The total number of records consumed : "+ poll.count());

                for(var record:poll){
                    //send the record to the opensearch endpoint
                    IndexRequest indexReq = new IndexRequest(
                        RESOURCE_LOCATION).source(record.value(), XContentType.JSON);

                    IndexResponse indexRes = openSearchClient.index(indexReq, RequestOptions.DEFAULT); 
                    log.info("Response id : "+ indexRes.getId());
                }
            }
            
        } 
    }
}
