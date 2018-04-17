package io.confluent.streams.zephyrstores;

import io.confluent.rest.Application;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.core.Configurable;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.servlet.ServletProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZephyrStoresApplication extends Application<ZephyrRestConfig> implements
    StatestoreExposer {

  private static final Logger log = LoggerFactory.getLogger(ZephyrStoresApplication.class);

  ZephyrStoresApplication app;
  StatestoreExposer exposer;

  public ZephyrStoresApplication() {
    super(new ZephyrRestConfig());
  }

  public ZephyrStoresApplication(ZephyrRestConfig config) {
    super(config);
  }

  @Override
  public void setupResources(Configurable<?> configurable, ZephyrRestConfig zephyrRestConfig) {
    exposer = new ZephyrResource(zephyrRestConfig);
    configurable.register(exposer);
    configurable.property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "/(static/.*|.*\\.html|)");
  }

  public void start() {
    try {
      // This simple configuration is driven by the command line. Run with an argument to specify
      // the format of the message returned by the API, e.g.
      // java -jar rest-utils-examples.jar \
      //    io.confluent.rest.examples.helloworld.HelloWorldApplication 'Goodbye, %s'
      TreeMap<String, String> settings = new TreeMap<String, String>();
//      if (args.length > 0) {
//        settings.put(HelloWorldRestConfig.GREETING_CONFIG, args[0]);
//      }
      ZephyrRestConfig config = new ZephyrRestConfig(settings);
      app = new ZephyrStoresApplication(config);
      app.start();
      log.info("Server started, listening for requests...");
//      app.join();
//    } catch (RestConfigException e) {
//      log.error("Server configuration failed: " + e.getMessage());
//      System.exit(1);
    } catch (Exception e) {
      log.error("Server died unexpectedly: " + e.toString());
    }

  }

  public void shutdown() {
    app.shutdown();
  }

  public void exposeAll(Map<String, ReadOnlyKeyValueStore> allStateStores) {
    exposer.exposeAll(allStateStores);
  }

  @Override
  public void expose(String exposedName, ReadOnlyKeyValueStore ss) {
    assert (ss != null);
    exposer.expose(exposedName, ss);
  }


}
