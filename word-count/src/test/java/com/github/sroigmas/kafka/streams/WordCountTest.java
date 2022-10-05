package com.github.sroigmas.kafka.streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WordCountTest {

  TopologyTestDriver testDriver;
  TestInputTopic<String, String> inputTopic;
  TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  public void initTestDriver() {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-test");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-host:9092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    Topology topology = WordCount.createTopology();
    testDriver = new TopologyTestDriver(topology, properties);
    inputTopic = testDriver.createInputTopic("word-count-input",
        new StringSerializer(), new StringSerializer());
    outputTopic = testDriver.createOutputTopic("word-count-output",
        new StringDeserializer(), new LongDeserializer());
  }

  @AfterEach
  public void closeTestDriver() {
    testDriver.close();
  }

  @Test
  public void assertCountsAreCorrect() {
    String firstExample = "testing Kafka Streams";
    inputTopic.pipeInput(firstExample);
    Assertions.assertEquals(new KeyValue<>("testing", 1L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("kafka", 1L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("streams", 1L), outputTopic.readKeyValue());

    String secondExample = "testing Kafka again";
    inputTopic.pipeInput(secondExample);
    Assertions.assertEquals(new KeyValue<>("testing", 2L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("kafka", 2L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("again", 1L), outputTopic.readKeyValue());
  }

  @Test
  public void assertRecordsAreLowercase() {
    String record = "KAFKA kafka Kafka";
    inputTopic.pipeInput(record);
    Assertions.assertEquals(new KeyValue<>("kafka", 1L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("kafka", 2L), outputTopic.readKeyValue());
    Assertions.assertEquals(new KeyValue<>("kafka", 3L), outputTopic.readKeyValue());
  }
}
