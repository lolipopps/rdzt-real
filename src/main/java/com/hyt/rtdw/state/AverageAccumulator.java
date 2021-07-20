package com.hyt.rtdw.state;

import java.io.Serializable;


public class AverageAccumulator implements Serializable {

    private long count = 0;

    private double sum = 0.0;

    public Double getLocalValue() {
        if (this.count == 0) {
            return 0.0;
        }
        return this.sum / this.count;
    }

    public void add(int value) {
        this.count++;
        this.sum += value;
    }

    public void add(long count, double sum) {
        this.count += count;
        this.sum += sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }
}
