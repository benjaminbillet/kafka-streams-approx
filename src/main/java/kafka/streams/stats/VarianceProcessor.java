package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.state.KeyValueStore;

public class VarianceProcessor<K, V> extends AbstractStatProcessor<K, V, Variance> {

  public VarianceProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    super(valueExtractor, outputKeyMapper, storeName);
  }

  @Override
  protected Variance getStoredStat(KeyValueStore<String, Variance> store) {
    Variance variance = store.get("variance");
    if (variance == null) {
      return new Variance();
    }
    return variance;
  }

  @Override
  protected void storeStat(Variance stat, KeyValueStore<String, Variance> store) {
    store.put("variance", stat);
  }
}
