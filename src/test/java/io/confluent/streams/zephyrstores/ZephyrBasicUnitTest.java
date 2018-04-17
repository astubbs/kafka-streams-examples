/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.streams.zephyrstores;

import io.confluent.examples.streams.WordCountLambdaExample;
import io.confluent.examples.streams.WordCountScalaIntegrationTest;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test based on {@link WordCountLambdaExample}, using an embedded Kafka
 * cluster.
 *
 * See {@link WordCountLambdaExample} for further documentation.
 *
 * See {@link WordCountScalaIntegrationTest} for the equivalent Scala example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ZephyrBasicUnitTest {

//  @ClassRule
//  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
  }

  @Test
  public void basicQuery() throws Exception {
    Properties props = new Properties();
//    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
//    props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
//    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    StreamsBuilder builder = new StreamsBuilder();

    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();

//    String questionsAwaitingAnswersStoreName = "questions-awaiting-answers-store";
//    StoreBuilder store = Stores.keyValueStoreBuilder(
//        Stores.persistentKeyValueStore(questionsAwaitingAnswersStoreName),
//        genericAvroSerde,
//        genericAvroSerde);
//    builder.addStateStore(store);

    KTable<Object, Object> table = builder.table(inputTopic, Materialized.as("input-store"));
    Topology topology = builder.build();
    String storeName = table.queryableStoreName();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());

    TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<String, String>(
        inputTopic,
        new StringSerializer(), new StringSerializer());

    StringSerializer strSerializer = new StringSerializer();
    StringDeserializer strDeserializer = new StringDeserializer();

    testDriver.pipeInput(factory.create(inputTopic, "key1", "value1"));

    ProducerRecord<String, String> record1 = testDriver
        .readOutput("output-topic-1", strDeserializer, strDeserializer);
    ProducerRecord<String, String> record2 = testDriver
        .readOutput("output-topic-1", strDeserializer, strDeserializer);
    ProducerRecord<String, String> record3 = testDriver
        .readOutput("output-topic-2", strDeserializer, strDeserializer);

    KeyValueStore stateStore = testDriver.getKeyValueStore(storeName);
    Map<String, StateStore> allStateStores = testDriver.getAllStateStores();
    Object key1 = stateStore.get("key1");
    assertThat(key1).isEqualTo("value1");

    /////////


    ZephyrStoresApplication zephyrStores = new ZephyrStoresApplication();
    zephyrStores.expose(storeName, stateStore);
//    zephyrStores.exposeAll(allStateStores);
    zephyrStores.start();




    assertThat(key1).isEqualTo("value1");
  }

  @Test
  public void shouldCountWords() throws Exception {
    List<String> inputValues = Arrays.asList(
        "Hello Kafka Streams",
        "All streams lead to Kafka",
        "Join Kafka Summit",
        "И теперь пошли русские слова"
    );
    List<KeyValue<String, Long>> expectedWordCounts = Arrays.asList(
        new KeyValue<>("hello", 1L),
        new KeyValue<>("all", 1L),
        new KeyValue<>("streams", 2L),
        new KeyValue<>("lead", 1L),
        new KeyValue<>("to", 1L),
        new KeyValue<>("join", 1L),
        new KeyValue<>("kafka", 3L),
        new KeyValue<>("summit", 1L),
        new KeyValue<>("и", 1L),
        new KeyValue<>("теперь", 1L),
        new KeyValue<>("пошли", 1L),
        new KeyValue<>("русские", 1L),
        new KeyValue<>("слова", 1L)
    );

//    assertThat(actualWordCounts).containsExactlyElementsOf(expectedWordCounts);
  }

}
