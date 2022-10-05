import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class UserDataProducer {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

    Producer<String, String> producer = new KafkaProducer<>(properties);

    /*
    FYI - We do .get() to ensure that the writes to the topics are sequential,
    for the sake of the teaching exercise.
    Do not do this in production or in any producer. Blocking a future is bad!
    */

    // 1 - we create a new user, then we send some data to Kafka
    System.out.println("\nExample 1 - new user\n");
    producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com"))
        .get();
    producer.send(purchaseRecord("john", "Apples and Bananas (1)"))
        .get();
    Thread.sleep(10000);

    // 2 - we receive user purchase, but it doesn't exist in Kafka
    System.out.println("\nExample 2 - non existing user\n");
    producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)"))
        .get();
    Thread.sleep(10000);

    // 3 - we update user "john", and send a new transaction
    System.out.println("\nExample 3 - update to user\n");
    producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com"))
        .get();
    producer.send(purchaseRecord("john", "Oranges (3)"))
        .get();
    Thread.sleep(10000);

    // 4 - we send a user purchase for stephane, but it exists in Kafka later
    System.out.println("\nExample 4 - non existing user then user\n");
    producer.send(purchaseRecord("stephane", "Computer (4)"))
        .get();
    producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph"))
        .get();
    producer.send(purchaseRecord("stephane", "Books (4)"))
        .get();
    producer.send(userRecord("stephane", null))
        .get(); // delete for cleanup
    Thread.sleep(10000);

    // 5 - we create a user, but it gets deleted before any purchase comes through
    System.out.println("\nExample 5 - user then delete then data\n");
    producer.send(userRecord("alice", "First=Alice"))
        .get();
    producer.send(userRecord("alice", null))
        .get(); // that's the deleted record
    producer.send(purchaseRecord("alice", "Apache Kafka Series (5)"))
        .get();
    Thread.sleep(10000);

    System.out.println("End of demo");
    producer.close();
  }

  private static ProducerRecord<String, String> userRecord(String key, String value) {
    return new ProducerRecord<>("user-data", key, value);
  }


  private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
    return new ProducerRecord<>("user-purchases", key, value);
  }
}
