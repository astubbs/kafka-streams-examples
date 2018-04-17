package io.confluent.streams.zephyrstores;

import java.util.Map;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public interface StatestoreExposer {

  void expose(String exposedName, ReadOnlyKeyValueStore ss);

  public void exposeAll(Map<String, ReadOnlyKeyValueStore> allStateStores);
}
