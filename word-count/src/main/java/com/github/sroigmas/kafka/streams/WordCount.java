package com.github.sroigmas.kafka.streams;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public class WordCount {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    String topic = "word-count-input";
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> stream = builder.stream(topic);

    KTable<String, Long> wordCounts = stream.mapValues(value -> value.toLowerCase())
        .flatMapValues(value -> Arrays.asList(value.split(" ")))
        .selectKey((key, value) -> value)
        .groupByKey()
        .count(Named.as("Counts"));

    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();

    System.out.println(topology.describe());

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
