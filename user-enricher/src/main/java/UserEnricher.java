import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

public class UserEnricher {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-enricher");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    //properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    String usersTopic = "user-data";
    String purchasesTopic = "user-purchases";

    StreamsBuilder builder = new StreamsBuilder();
    GlobalKTable<String, String> usersTable = builder.globalTable(usersTopic);
    KStream<String, String> purchasesStream = builder.stream(purchasesTopic);

    KStream<String, String> enrichedPurchasesInnerJoinStream = purchasesStream.join(usersTable,
        (key, value) -> key,
        (purchase, user) -> "Purchase: " + purchase + ", User: " + user);
    enrichedPurchasesInnerJoinStream.to("user-purchases-enriched-inner-join");

    KStream<String, String> enrichedPurchasesLeftJoinStream = purchasesStream.leftJoin(usersTable,
        (key, value) -> key,
        (purchase, user) -> {
          String value = "Purchase: " + purchase + ", User: ";
          return value.concat(Objects.isNull(user) ? "null" : user);
        });
    enrichedPurchasesLeftJoinStream.to("user-purchases-enriched-left-join");

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
