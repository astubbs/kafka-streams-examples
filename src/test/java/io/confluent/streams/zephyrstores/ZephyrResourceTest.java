/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.streams.zephyrstores;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import java.util.Properties;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;

/**
 * This tests a single resource by setting up a Jersey-based test server embedded in the application.
 * This is intended to unit test a single resource class, not the entire collection of resources.
 * EmbeddedServerTestHarness does most of the heavy lifting so you only need to issue requests and
 * check responses.
 */
public class ZephyrResourceTest extends EmbeddedServerTestHarness<ZephyrRestConfig, ZephyrStoresApplication> {
  private final static String mediatype = "application/vnd.hello.v1+json";

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  ZephyrResource resource;

  public ZephyrResourceTest() throws RestConfigException {
    // We need to specify which resources we want available, i.e. the ones we need to test. If we need
    // access to the server Configuration, as HelloWorldResource does, a default config is available
    // in the 'config' field.
    resource = new ZephyrResource(config);
    addResource(resource);
  }

  @Test
  public void testHello() {
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
//    Map<String, ReadOnlyKeyValueStore> allStateStores = testDriver.getAllStateStores();
    Object key1 = stateStore.get("key1");
    assertThat(key1).isEqualTo("value1");

    /////////


    resource.expose(storeName, stateStore);
//    resource.exposeAll(allStateStores);



    assertThat(key1).isEqualTo("value1");


    //////


    String acceptHeader = mediatype;
    String contextPath = "/" + storeName;
    Response response = request(contextPath, acceptHeader, "key", "key1").get();
    // The response should indicate success and have the expected content type
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(mediatype, response.getMediaType().toString());

    // We should also be able to parse it as the expected output format
    final ZephyrResource.HelloResponse message = response.readEntity(ZephyrResource.HelloResponse.class);
    // And it should contain the expected message
    assertEquals("Hello, World!", message.getMessage());
  }

  protected Invocation.Builder request(String target, String mediatype, String param, String value) {
    Invocation.Builder builder = getJerseyTest().target(target).queryParam(param, value).request();
    if (mediatype != null) {
      builder.accept(mediatype);
    }
    return builder;
  }

}
