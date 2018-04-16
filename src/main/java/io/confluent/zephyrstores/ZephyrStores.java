package io.confluent.zephyrstores;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class ZephyrStores {

    class ExposedStore {
    }

    public ExposedStore expose(ReadOnlyKeyValueStore ss) {
        return null;
    }
}
