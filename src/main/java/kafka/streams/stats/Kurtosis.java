package kafka.streams.stats;

import javax.validation.constraints.NotNull;

public class Kurtosis implements StatAccumulator {
  private static final long serialVersionUID = -4667733586146384371L;

  private Mean mean = new Mean();
  private double secondCentralMoment = 0;
  private double thirdCentralMoment = 0;
  private double fourthCentralMoment = 0;

  public void add(@NotNull double x) {
    double delta = x - mean.get();
    double delta2 = delta * delta;
    double n = mean.getCount() + 1;
    double n2 = n * n;
    double m2 = getSecondCentralMoment();
    double m3 = getThirdCentralMoment();

    double term1 = delta * (delta * (n - 1) / n);
    fourthCentralMoment += term1 * delta2 / n2 * (n2 - 3 * n + 3) + (6 * delta2 * m2 / n2) - 4 * delta * m3 / n;
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

  public double getFourthCentralMoment() {
    return this.fourthCentralMoment;
  }

  public double get() {
    long n = getCount();
    double m2 = getSecondCentralMoment();
    double m4 = getFourthCentralMoment();

    return (m4 * n) / (m2 * m2) - 3d;
  }
}
