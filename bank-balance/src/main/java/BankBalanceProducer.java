import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class BankBalanceProducer {

  private static final List<String> customers = List.of("John", "Jane", "David", "Sam", "Claire",
      "Rose");

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    for (int i = 1; i <= 100; i++) {
      producer.send(createRecord());
    }
    producer.close();
  }

  private static ProducerRecord<String, String> createRecord() {
    Random random = new Random();
    String customer = customers.get(random.nextInt(customers.size()));
    Integer amount = random.nextInt(1000);

    ObjectNode transaction = JsonNodeFactory.instance.objectNode()
        .put("name", customer)
        .put("amount", amount)
        .put("time", LocalDateTime.now().toString());

    String topic = "bank-balance-input";
    return new ProducerRecord<>(topic, customer,
        transaction.toString());
  }
}
