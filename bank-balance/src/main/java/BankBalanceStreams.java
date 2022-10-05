import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class BankBalanceStreams {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2);

    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    String topic = "bank-balance-input";
    StreamsBuilder builder = new StreamsBuilder();
    Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
    KStream<String, JsonNode> stream = builder.stream(topic,
        Consumed.with(Serdes.String(), jsonSerde));

    KTable<String, JsonNode> table = stream.groupByKey(Grouped.with(Serdes.String(), jsonSerde))
        .aggregate(
            () -> JsonNodeFactory.instance.objectNode()
                .put("count", 0)
                .put("balance", 0)
                .put("time", Instant.ofEpochMilli(0L).toString()),
            (key, newValue, aggValue) -> JsonNodeFactory.instance.objectNode()
                .put("count", aggValue.get("count").asInt() + 1)
                .put("balance", aggValue.get("balance").asInt() + newValue.get("amount").asInt())
                .put("time", newValue.get("time").asText()),
            Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                .withKeySerde(Serdes.String())
                .withValueSerde(jsonSerde));

    table.toStream().to("bank-balance-output", Produced.with(Serdes.String(), jsonSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
