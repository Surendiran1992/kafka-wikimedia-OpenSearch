package wikimedia.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaConfig {
    public static final String CONDUCKTOR_SECOND_TOPIC = "second-topic";
    public static final String CONDUCKTOR_FIRST_TOPIC = "first_topic";
    public static final String CONDUCKTOR_THIRD_TOPIC = "third-topic";
    public static final String CONDUCKTOR_FOURTH_TOPIC = "fourth-topic";
    public static final String LOCALHOST_FIRST_TOPIC = "first_topic";
    public static final String LOCALHOST_SECOND_TOPIC = "second_topic";
    public static final String CONDUCTOR_BOOTSTRAP_SERVER = "selected-flea-7056-us1-kafka.upstash.io:9092";
    public static final String LOCALHOST_BOOTSTRAP_SERVER = "localhost:9092";

    public static Properties getLocalServerConsumerProperties(String groupId,String autoOffset,String keyDeSerializer, String valueDeSerializer,boolean autocommit) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer);
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,autoOffset);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(autocommit));
        //it waits maximum of 5s before fetching the data and it overiddes MAX and MIN FETCH SIZE, if minimum fetch size is not attained in 5s this will return accumulated data
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5000"); 
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"50000"); //waits for 5KB of data to be accumulated before it fetches it
        return props;
    }

    public static Properties getConduktorConsumerProperties(String keySerializer, String valueSerializer) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "selected-flea-7056-us1-kafka.upstash.io:9092");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"c2VsZWN0ZWQtZmxlYS03MDU2JOjhECcKpUHdcwRoBOex-NgseRbCBjru24zgWNo\" password=\"Njc3YTJjYTItMmRmMy00MzZhLThiMDQtNDY2MTg2YTI3OTZk\";");
        props.setProperty("key.serializer", keySerializer);
        props.setProperty("value.serializer", valueSerializer);
        return props;
    }
    
}
