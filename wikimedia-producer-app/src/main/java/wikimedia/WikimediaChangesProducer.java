package wikimedia;

import static wikimedia.kafka.KafkaConfig.*;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventSource;

import wikimedia.kafka.KafkaConfig;

public class WikimediaChangesProducer {
    private static final String WIKIMEDIA_TOPIC ="wikimedia.change.stream";
    private static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    public static void main(String[] args) throws InterruptedException {
        
        var props = KafkaConfig.getLocalServerProducerProperties(StringSerializer.class.getName(),  StringSerializer.class.getName()); 

        // //for >2.8 the below properties should be set for safe producer otherwise there will be dataloss
        // props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // props.setProperty(ProducerConfig.ACKS_CONFIG, "-1"); //equals to acks=all
        // props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //adding compression and increasig batchSize to 32KB for better efficiency
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    
    
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        
        WikimediaChangesHandler eventHandler = new WikimediaChangesHandler(producer, WIKIMEDIA_TOPIC);
        EventSource.Builder sourceBuilder = new EventSource.Builder(eventHandler, URI.create(WIKIMEDIA_URL));
        EventSource build = sourceBuilder.build();

        // start the producer in another thread, produce for 10 secs and close the connection
        build.start();

        TimeUnit.SECONDS.sleep(3);

    }
}
