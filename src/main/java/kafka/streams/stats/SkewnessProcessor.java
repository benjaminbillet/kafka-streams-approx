package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.state.KeyValueStore;

public class SkewnessProcessor<K, V> extends AbstractStatProcessor<K, V, Skewness> {

  public SkewnessProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    super(valueExtractor, outputKeyMapper, storeName);
  }

  @Override
  protected Skewness getStoredStat(KeyValueStore<String, Skewness> store) {
    Skewness skewness = store.get("skewness");
    if (skewness == null) {
      return new Skewness();
    }
    return skewness;
  }

  @Override
  protected void storeStat(Skewness stat, KeyValueStore<String, Skewness> store) {
    store.put("skewness", stat);
  }
}
