package kafka.streams.stats;

import javax.validation.constraints.NotNull;

public class StandardDeviation implements StatAccumulator {
  private static final long serialVersionUID = 8083039306477734828L;

  private Variance variance = new Variance();

  public void add(@NotNull double x) {
    variance.add(x);
  }

  public long getCount() {
    return this.variance.getCount();
  }

  public double get() {
    return Math.sqrt(this.variance.get());
  }
}
