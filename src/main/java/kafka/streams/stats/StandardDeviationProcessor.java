package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.state.KeyValueStore;

public class StandardDeviationProcessor<K, V> extends AbstractStatProcessor<K, V, StandardDeviation> {

  public StandardDeviationProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    super(valueExtractor, outputKeyMapper, storeName);
  }

  @Override
  protected StandardDeviation getStoredStat(KeyValueStore<String, StandardDeviation> store) {
    StandardDeviation standardDeviation = store.get("standard-deviation");
    if (standardDeviation == null) {
      return new StandardDeviation();
    }
    return standardDeviation;
  }

  @Override
  protected void storeStat(StandardDeviation stat, KeyValueStore<String, StandardDeviation> store) {
    store.put("standard-deviation", stat);
  }
}
