package wikimedia;

import static wikimedia.kafka.KafkaConfig.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangesHandler implements EventHandler{
    private final Logger log = LoggerFactory.getLogger(getClass().getSimpleName());
    private KafkaProducer<String,String> producer;
    private String topic;

    public WikimediaChangesHandler(KafkaProducer<String,String> producer,String topic){
        this.producer = producer;
        this.topic = topic;
    }
    @Override
    public void onOpen() throws Exception {
       //do nothing
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        //do nothing
    }

    @Override
    public void onError(Throwable t) {
        log.info("Unexpected Error in the stream"+ t);
    }

    
}
