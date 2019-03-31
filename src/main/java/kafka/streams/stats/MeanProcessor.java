package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.state.KeyValueStore;

public class MeanProcessor<K, V> extends AbstractStatProcessor<K, V, Mean> {

  public MeanProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    super(valueExtractor, outputKeyMapper, storeName);
  }

  @Override
  protected Mean getStoredStat(KeyValueStore<String, Mean> store) {
    Mean mean = store.get("mean");
    if (mean == null) {
      return new Mean();
    }
    return mean;
  }

  @Override
  protected void storeStat(Mean stat, KeyValueStore<String, Mean> store) {
    store.put("mean", stat);
  }
}
