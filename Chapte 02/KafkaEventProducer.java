import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaEventProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String topic = "user.registration";
        String event = "{\"event_type\": \"User-Registration\", \"user_id\": \"99999\", \"timestamp\": \"2024-10-05T15:23:30\"}";

        producer.send(new ProducerRecord<>(topic, event));
        producer.close();

        System.out.println("Event sent to Kafka!");
    }
}

