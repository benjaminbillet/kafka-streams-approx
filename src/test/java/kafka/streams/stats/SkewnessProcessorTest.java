package kafka.streams.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import kafka.streams.serdes.ObjectSerdeFactory;

public class SkewnessProcessorTest {

  private MockProcessorContext processorContext;
  private SkewnessProcessor<String, Integer> processor;

  @BeforeEach
  void setUp() throws Exception {
    String storeName = "skewness-store-" + UUID.randomUUID();
    processor = new SkewnessProcessor<>((k, v) -> v.doubleValue(), (k, v) -> "skewness", storeName);

    Properties config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "skewness-test");
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "nowhere:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    KeyValueStore<String, Skewness> store = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName),
        Serdes.String(), ObjectSerdeFactory.createSerde(Skewness.class)).withLoggingDisabled().build();

    processorContext = new MockProcessorContext(config);
    store.init(processorContext, store);
    processorContext.register(store, null);
    processor.init(processorContext);
  }

  @Test
  void testSkewness() {
    int n = 5;
    IntStream.range(0, n).forEach(x -> {
      processor.process(null, x);
    });

    double average = IntStream.range(0, n).average().getAsDouble();
    double variance = IntStream.range(0, n).mapToDouble(x -> Math.pow(x - average, 2)).sum() / (n - 1);

    double skewness = IntStream.range(0, n).mapToDouble(x -> Math.pow(x - average, 3)).sum() / (n - 1);
    skewness = skewness / ((n - 1) * Math.pow(variance, 3));
    skewness = Math.sqrt(skewness);

    List<CapturedForward> output = processorContext.forwarded();
    assertEquals(n, output.size());
    assertEquals("skewness", output.get(n - 1).keyValue().key);
    assertEquals(skewness, (double) output.get(n - 1).keyValue().value, 0.00001);
  }
}
