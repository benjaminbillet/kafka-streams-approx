package kafka.streams.stats;

import javax.validation.constraints.NotNull;

public class Skewness implements StatAccumulator {
  private static final long serialVersionUID = -2957931834455982101L;

  private Mean mean = new Mean();
  private double secondCentralMoment = 0;
  private double thirdCentralMoment = 0;

  public void add(@NotNull double x) {
    double delta = x - mean.get();
    double n = mean.getCount() + 1;
    double m2 = getSecondCentralMoment();

    double term1 = delta * (delta * (n - 1) / n);
    thirdCentralMoment += term1 * (delta * (n - 2) / n) - 3 * (delta * m2) / n;
    secondCentralMoment += term1;
    mean.add(x);
  }

  public long getCount() {
    return this.mean.getCount();
  }

  public double getSecondCentralMoment() {
    return this.secondCentralMoment;
  }

  public double getThirdCentralMoment() {
    return this.thirdCentralMoment;
  }

  public double get() {
    long n = getCount();
    double m2 = getSecondCentralMoment();
    double m3 = getThirdCentralMoment();

    return Math.sqrt(n) * m3 / Math.pow(m2, 1.5);
  }
}
