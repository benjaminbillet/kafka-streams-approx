package kafka.streams.stats;

import javax.validation.constraints.NotNull;

public class Variance implements StatAccumulator {
  private static final long serialVersionUID = -7460793864409287785L;

  private Mean mean = new Mean();
  private double secondCentralMoment = 0;

  public void add(@NotNull double x) {
    double delta = x - mean.get();
    double n = mean.getCount() + 1;
    secondCentralMoment += delta * (delta * (n - 1) / n);
    mean.add(x);
  }

  public long getCount() {
    return this.mean.getCount();
  }

  public double get() {
    return this.secondCentralMoment / (getCount() - 1);
  }

  public double getSecondCentralMoment() {
    return this.secondCentralMoment;
  }
}
