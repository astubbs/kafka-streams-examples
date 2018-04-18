package io.confluent.streams.zephyrstores;

import avro.shaded.com.google.common.collect.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Produces("application/vnd.hello.v1+json")
public class ZephyrResource implements StatestoreExposer {

  private static final Logger log = LoggerFactory.getLogger(ZephyrResource.class);

  ZephyrRestConfig config;

  Map<String, ReadOnlyKeyValueStore> stores;

  public ZephyrResource(ZephyrRestConfig config) {
    this.config = config;
    this.stores = new HashMap();

//    config.register(ValidationFeature.class);
//    config.register(ConstraintViolationExceptionMapper.class);
//    config.register(new WebApplicationExceptionMapper(restConfig));
//    config.register(new GenericExceptionMapper(restConfig));

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
  @Path("/{storeName}")
  @PerformanceMetric("get-all")
  public List getAll(@NotNull @PathParam("storeName") String storeName,
      @Context Request request) {

    ReadOnlyKeyValueStore store = this.stores.get(storeName);
    if (store == null) {
      throw Errors.storeNotFoundException(storeName);
    }

    log.debug(String.format("Requested all entries in store: %s", storeName));

    KeyValueIterator all = store.all();
    List allaslist = Lists.newArrayList(all);

    return allaslist;
  }

  @GET()
  @Path("/{storeName}/{key}")
  @PerformanceMetric("get-key")
  public HelloResponse get(@NotNull @PathParam("storeName") String storeName,
      @NotNull @PathParam("key") String key, @Context Request request) {

    // shouldn't be required
//    if (StringUtils.isBlank(key)) {
//      throw Errors.keyMissing
//    }

    ReadOnlyKeyValueStore store = this.stores.get(storeName);
    if (store == null) {
      throw Errors.storeNotFoundException(storeName);
    }

    Object value = store.get(key);
    if (value == null) {
      throw Errors.keyNotFoundException(key, storeName);
    }
    // Use a configuration setting to control the message that's written. The name is extracted from
    // the query parameter "name", or defaults to "World". You can test this API with curl:
    // curl http://localhost:8080/hello
    //   -> {"message":"Hello, World!"}
    // curl http://localhost:8080/hello?name=Bob
    //   -> {"message":"Hello, Bob!"}
    log.debug("requested: " + key + " v: " + value);
    return new HelloResponse(
//        String.format(config.getString(HelloWorldRestConfig.GREETING_CONFIG),
//            (name == null ? "World" : name)));
        //"requested: " + key + " v: " + value);
        value.toString());
  }
}
