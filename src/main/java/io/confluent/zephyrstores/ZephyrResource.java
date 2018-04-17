package io.confluent.zephyrstores;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.hibernate.validator.constraints.NotEmpty;

@Path("/hello")
@Produces("application/vnd.hello.v1+json")
public class ZephyerResource implements StatestoreExposer {

  ZephyrRestConfig config;

  Map stores;

  public ZephyerResource(ZephyrRestConfig config) {
    this.config = config;
    this.stores = Collections.emptyMap();
  }

  @Override
  public void expose(String exposedName, ReadOnlyKeyValueStore ss) {
    stores.put(exposedName, ss);
  }

  @Override
  public void exposeAll(Map<String, StateStore> allStateStores) {
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

  @GET
  @PerformanceMetric("key-get")
  public HelloResponse get(@QueryParam("key") String key) {
    // Use a configuration setting to control the message that's written. The name is extracted from
    // the query parameter "name", or defaults to "World". You can test this API with curl:
    // curl http://localhost:8080/hello
    //   -> {"message":"Hello, World!"}
    // curl http://localhost:8080/hello?name=Bob
    //   -> {"message":"Hello, Bob!"}
    return new HelloResponse(
//        String.format(config.getString(HelloWorldRestConfig.GREETING_CONFIG),
//            (name == null ? "World" : name)));
        "requested: "+key);
  }
}
