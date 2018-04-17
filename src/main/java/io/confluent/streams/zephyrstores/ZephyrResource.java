package io.confluent.streams.zephyrstores;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.hibernate.validator.constraints.NotEmpty;

@Path("/")
@Produces("application/vnd.hello.v1+json")
public class ZephyrResource implements StatestoreExposer {

  ZephyrRestConfig config;

  Map<String, ReadOnlyKeyValueStore> stores;

  public ZephyrResource(ZephyrRestConfig config) {
    this.config = config;
    this.stores = new HashMap();
  }

  @Override
  public void expose(String exposedName, ReadOnlyKeyValueStore ss) {
    stores.put(exposedName, ss);
  }

  @Override
  public void exposeAll(Map<String, ReadOnlyKeyValueStore> allStateStores) {
    stores.putAll(allStateStores);
  }

  /**
   * A simple response entity with validation constraints.
   */
  public static class HelloResponse {

    @NotEmpty
    private String message;

    public HelloResponse() { /* Jackson deserialization */ }

    public HelloResponse(String message) {
      this.message = message;
    }

    @JsonProperty
    public String getMessage() {
      return message;
    }
  }

  @GET()
  @Path("{storeName}/")
  @PerformanceMetric("key-get")
  public HelloResponse get(@PathParam("storeName") String storeName,
      @QueryParam("key") String key, @Context Request request) {
    if (key == null) {
      throw new RuntimeException("Null key");
    }
    ReadOnlyKeyValueStore store = this.stores.get(storeName);
    Object value = store.get(key);
    // Use a configuration setting to control the message that's written. The name is extracted from
    // the query parameter "name", or defaults to "World". You can test this API with curl:
    // curl http://localhost:8080/hello
    //   -> {"message":"Hello, World!"}
    // curl http://localhost:8080/hello?name=Bob
    //   -> {"message":"Hello, Bob!"}
    return new HelloResponse(
//        String.format(config.getString(HelloWorldRestConfig.GREETING_CONFIG),
//            (name == null ? "World" : name)));
        "requested: " + key + " v: " + value);
  }
}
