package kafka.streams.stats;

import javax.validation.constraints.NotNull;

public class Mean implements StatAccumulator {
  private static final long serialVersionUID = -4941389374784688616L;

  private long count = 0l;
  private double mean = 0;

  public void add(@NotNull double x) {
    mean = (mean * count + x) / (count + 1);
    count++;
  }

  public long getCount() {
    return this.count;
  }

  public double get() {
    return this.mean;
  }
}
