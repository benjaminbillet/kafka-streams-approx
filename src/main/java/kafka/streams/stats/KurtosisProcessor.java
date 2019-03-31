package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.state.KeyValueStore;

public class KurtosisProcessor<K, V> extends AbstractStatProcessor<K, V, Kurtosis> {

  public KurtosisProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    super(valueExtractor, outputKeyMapper, storeName);
  }

  @Override
  protected Kurtosis getStoredStat(KeyValueStore<String, Kurtosis> store) {
    Kurtosis kurtosis = store.get("kurtosis");
    if (kurtosis == null) {
      return new Kurtosis();
    }
    return kurtosis;
  }

  @Override
  protected void storeStat(Kurtosis stat, KeyValueStore<String, Kurtosis> store) {
    store.put("kurtosis", stat);
  }
}
