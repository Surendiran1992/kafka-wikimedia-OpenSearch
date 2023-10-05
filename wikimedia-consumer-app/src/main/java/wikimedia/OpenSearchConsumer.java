package wikimedia;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import static java.lang.String.format;
import wikimedia.kafka.KafkaConfig;

public class OpenSearchConsumer {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
    // private static RestHighLevelClient createOpenSearchClient;
    private static final String GROUP_ID = "opensearch-consumer-group";
    private static final String OFFSET_CONFIG = "latest";
    private static final String TOPIC = "wikimedia.change.stream";
    private static String RESOURCE_LOCATION = "wikimedia";

    public static void main(String[] args) throws IOException {
        var openSearchClient = OpenSearchClient.createOpenSearchClient();
        // KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        // KafkaConfig.getLocalServerConsumerProperties(GROUP_ID, OFFSET_CONFIG,
        // StringDeserializer.class.getName(), StringDeserializer.class.getName(),
        // true));

        // consumer with auto commit disabled, will commit offset manually after processing
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                KafkaConfig.getLocalServerConsumerProperties(GROUP_ID, OFFSET_CONFIG,
                        StringDeserializer.class.getName(), StringDeserializer.class.getName(), false));

        shutdownHook(consumer);

        try (openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(RESOURCE_LOCATION),
                    RequestOptions.DEFAULT);
            if (!indexExists) {
                var req = new CreateIndexRequest(RESOURCE_LOCATION);
                openSearchClient.indices().create(req, RequestOptions.DEFAULT);
                log.info(format("The new index %s is created", RESOURCE_LOCATION));
            } else {
                log.info(format("The given index %s already exists", RESOURCE_LOCATION));
            }

            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                // processIndividualRecord(consumer, openSearchClient);
                processBulkRecord(consumer, openSearchClient);
            }

        }catch (WakeupException wake){log.info("Consumer is starting to shutdown");
        }catch (Exception e){log.error("Unexpected exception");
        }finally {
            consumer.close();
            openSearchClient.close();
            log.info("The consumer is now gracefully shutdown");
        }
    }

    private static void processIndividualRecord(KafkaConsumer<String, String> consumer, RestHighLevelClient client)
            throws IOException {
        var isClientExcepThrown = false;
        do {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(3000));
            log.info("The total number of records consumed : " + poll.count());
            Map<TopicPartition, OffsetAndMetadata> offsetAndPartition = new HashMap<>();
            try {
                for (var record : poll) {
                    String id = extractIdInRequest(record.value());
                    offsetAndPartition = Map.of(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()));
                    // send the record to the opensearch endpoint
                    IndexRequest indexReq = new IndexRequest(
                            RESOURCE_LOCATION).source(record.value(), XContentType.JSON)
                            .id(id);
                    IndexResponse indexRes;
                    indexRes = client.index(indexReq, RequestOptions.DEFAULT);
                    log.info("Response id : " + indexRes.getId());
                }

                consumer.commitSync();
                log.info("Successfully Committed the offset");
            } catch (OpenSearchStatusException e) {
                isClientExcepThrown = true;
                consumer.commitSync(offsetAndPartition,Duration.ofMillis(3000));
                log.info(format("Exception thrown while processing record, Sucessfully Committed the offset"));
            }
        } while (isClientExcepThrown);
    }

    private static void processBulkRecord(KafkaConsumer<String, String> consumer, RestHighLevelClient client)
            throws IOException {
        var isClientExcepThrown = false;
        BulkRequest bulkReq = new BulkRequest();

        do {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(3000));
            log.info("The total number of records consumed : " + poll.count());
            try {
                for (var record : poll) {
                    String id = extractIdInRequest(record.value());
                    // send the record to the opensearch endpoint

                    IndexRequest indexReq = new IndexRequest(
                            RESOURCE_LOCATION).source(record.value(), XContentType.JSON)
                            .id(id);

                    bulkReq.add(indexReq);
                }
                if (bulkReq.numberOfActions() > 0) {
                    BulkResponse bulkRes = client.bulk(bulkReq, RequestOptions.DEFAULT);
                    log.info(format("Inserted %d records", bulkRes.getItems().length));
                    consumer.commitSync();
                    log.info("Successfully Committed the offset");
                }
            } catch (OpenSearchStatusException e) {
                isClientExcepThrown = true;
                consumer.commitSync();
                log.info(format("Exception thrown while processing record, Sucessfully Committed the offset"));
            }
        } while (isClientExcepThrown);

    }

    private static String extractIdInRequest(String jsonRcrd) {
        return JsonParser.parseString(jsonRcrd).getAsJsonObject()
                .get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static void shutdownHook(KafkaConsumer<String,String> consumer){
         //reference to main thread
         Thread mainThread = Thread.currentThread();

         //creating new thread for shutdowm from the runTime
         Runtime.getRuntime().addShutdownHook(new Thread(){
             @Override
             public void run(){
                 log.info("ShutDown Detected lets shutdown gracefully by calling consumer.wakeup().........");
                 consumer.wakeup();
                 //this will allow shutdownHook to allow the execution in main thread
                 try {
                     mainThread.join();
                 } catch (InterruptedException e) {
                     throw new RuntimeException(e);
                 }
             }
         });
    }
}
