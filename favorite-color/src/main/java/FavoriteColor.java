import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class FavoriteColor {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    String topic = "favorite-color-input";
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> stream = builder.stream(topic);

    stream.filter((key, value) -> value.matches("\\w+,\\w+"))
        .selectKey((key, value) -> value.split(",")[0].toLowerCase())
        .mapValues(value -> value.split(",")[1].toLowerCase())
        .filter((key, value) -> Arrays.asList("green", "red", "blue").contains(value))
        .to("favorite-color-intermediary", Produced.with(Serdes.String(), Serdes.String()));

    KTable<String, String> table = builder.table("favorite-color-intermediary");
    KTable<String, Long> favoriteColors = table.groupBy(
            (key, value) -> new KeyValue<>(value, value))
        .count(Materialized.as("Counts"));

    favoriteColors.toStream()
        .to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
