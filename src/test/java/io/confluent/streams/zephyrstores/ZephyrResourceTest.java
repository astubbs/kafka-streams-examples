/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import javax.ws.rs.core.Response.Status;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.NotImplementedException;
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
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

public class ZephyrResourceTest extends
    EmbeddedServerTestHarness<ZephyrRestConfig, ZephyrStoresApplication> {

  private final static String mediatype = "application/vnd.hello.v1+json";

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  static ZephyrResource resource;

  public ZephyrResourceTest() throws RestConfigException {
    resource = new ZephyrResource(config);
    addResource(resource);
    setupResource();
  }

  private static TopologyTestDriver testDriver;
  private static String storeName;

  public static void setupResource() {
    StreamsBuilder builder = new StreamsBuilder();

    KTable<Object, Object> table = builder.table(inputTopic, Materialized.as("input-store"));
    Topology topology = builder.build();
    storeName = table.queryableStoreName();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    testDriver = new TopologyTestDriver(topology, config);

    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<String, String>(
        inputTopic,
        new StringSerializer(), new StringSerializer());

    StringSerializer strSerializer = new StringSerializer();
    StringDeserializer strDeserializer = new StringDeserializer();

    testDriver.pipeInput(factory.create(inputTopic, "key1", "value1"));

    KeyValueStore stateStore = testDriver.getKeyValueStore(storeName);
    resource.expose(storeName, stateStore);

    //    resource.exposeAll(allStateStores);
  }


  @Test
  public void testBasicStoreGet() {
    KeyValueStore stateStore = testDriver.getKeyValueStore(storeName);
//    Map<String, ReadOnlyKeyValueStore> allStateStores = testDriver.getAllStateStores();
    Object key1 = stateStore.get("key1");
    assertThat(key1).isEqualTo("value1");
  }

  private String acceptHeader = mediatype;
  String contextPath = "/" + storeName;

  @Test
  public void testBasicResourceGet() {
    String acceptHeader = mediatype;
    String contextPath = "/" + storeName;
    Response response = request(contextPath, acceptHeader, "key", "key1").get();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(mediatype, response.getMediaType().toString());

    // We should also be able to parse it as the expected output format
    final ZephyrResource.HelloResponse message = response
        .readEntity(ZephyrResource.HelloResponse.class);
    // And it should contain the expected message
    assertEquals("Hello, World!", message.getMessage());
  }

  @Test
  public void testGetWithMissingKey() {
    // missing key
    Response responseNoKey = request(contextPath, acceptHeader).get();
    assertEquals(Status.BAD_REQUEST.getStatusCode(), responseNoKey.getStatus());
  }

  @Test
  public void testGetWithWrongKeyParamName() {
    // missing key
    Response responseWrongKey = request(contextPath, acceptHeader, "key-missing", "wrong-key")
        .get();
    assertEquals(Status.BAD_REQUEST.getStatusCode(), responseWrongKey.getStatus());
  }

  @Test
  public void testGetWrongKey() {
    // wrong key
    Response responseWrongKeyTwo = request(contextPath, acceptHeader, "key", "wrong-key").get();
    assertEquals(Status.NO_CONTENT.getStatusCode(), responseWrongKeyTwo.getStatus());
  }

  @Test
  public void testGetWringKeyEmptyResponse() {
    // wrong key - empty response option
    Response response = request(contextPath, acceptHeader, "key", "wrong-key").get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ZephyrResource.HelloResponse messageWrongKeyThree = response
        .readEntity(ZephyrResource.HelloResponse.class);
    assertEquals("", messageWrongKeyThree.getMessage());
  }

  @Test
  public void testGetFromWindowStore() {
    // window store
    Response response = request(contextPath, acceptHeader, "key", "wrong-key").get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ZephyrResource.HelloResponse messageWrongKeyThree = response
        .readEntity(ZephyrResource.HelloResponse.class);
    assertEquals("", messageWrongKeyThree.getMessage());
  }

  @Test
  public void testGetWithRangeQuery() {
    // range query
    Response response = request(contextPath, acceptHeader, "key", "wrong-key").get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ZephyrResource.HelloResponse messageWrongKeyThree = response
        .readEntity(ZephyrResource.HelloResponse.class);
    assertEquals("", messageWrongKeyThree.getMessage());
  }

  @Test
  public void testGetAllKeys() {
    // get all keys
    Response response = request(contextPath, acceptHeader, "key", "wrong-key").get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ZephyrResource.HelloResponse messageWrongKeyThree = response
        .readEntity(ZephyrResource.HelloResponse.class);
    assertEquals("", messageWrongKeyThree.getMessage());
  }

//  @Test
//  public void testGetAvroFormat(){
//    throw new NotImplementedException();
//  }

  protected Invocation.Builder request(String target, String mediatype, String param,
      String value) {
    Invocation.Builder builder = getJerseyTest().target(target).queryParam(param, value).request();
    if (mediatype != null) {
      builder.accept(mediatype);
    }
    return builder;
  }

}
