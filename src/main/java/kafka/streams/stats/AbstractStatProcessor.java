package kafka.streams.stats;

import java.util.function.BiFunction;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

/**
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public abstract class AbstractStatProcessor<K, V, T extends StatAccumulator> implements Processor<K, V> {
  private ProcessorContext context;
  private String storeName;
  private KeyValueStore<String, T> store;
  private BiFunction<K, V, K> outputKeyMapper;
  private BiFunction<K, V, Double> valueExtractor;

  /**
   * @param valueExtractor   A function that can extracts a stream identifier
   *                            from a stream record.
   * @param outputKeyMapper     A function that supplies keys for the output
   *                            records.
   * @param storeName           The name of the store used to save the content of
   *                            the hash tables.
   */
  public AbstractStatProcessor(BiFunction<K, V, Double> valueExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    this.valueExtractor = valueExtractor;
    this.outputKeyMapper = outputKeyMapper;
    this.storeName = storeName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = (KeyValueStore<String, T>) context.getStateStore(storeName);

    Gauge.builder("store-size", this.store, store -> store.approximateNumEntries()).tag("store-name", storeName)
        .description("Approximate number of objects into the store").register(Metrics.globalRegistry);
  }

  @Override
  public void process(K key, V value) {
    T stat = getStoredStat(store);

    double v = valueExtractor.apply(key, value);
    stat.add(v);

    context.forward(outputKeyMapper.apply(key, value), stat.get());
    storeStat(stat, store);
  }

  protected abstract T getStoredStat(KeyValueStore<String, T> store);
  protected abstract void storeStat(T stat, KeyValueStore<String, T> store);

  @Override
  public void close() {
    // nothing to do
  }
}
