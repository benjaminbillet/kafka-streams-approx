package kafka.streams.stats;

import java.io.Serializable;

import javax.validation.constraints.NotNull;

public interface StatAccumulator extends Serializable {
  void add(@NotNull double x);
  double get();
}
