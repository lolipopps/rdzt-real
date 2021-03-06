package com.hyt.rtdw.config;

import java.util.List;

public class ClickhouseRequestBlank {
    private final List<String> values;
    private final String targetTable;
    private int attemptCounter;

    public ClickhouseRequestBlank(List<String> values, String targetTable) {
        this.values = values;
        this.targetTable = targetTable;
        this.attemptCounter = 0;
    }

    public List<String> getValues() {
        return values;
    }

    public void incrementCounter() {
        this.attemptCounter++;
    }

    public int getAttemptCounter() {
        return attemptCounter;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public static final class Builder {
        private List<String> values;
        private String targetTable;

        private Builder() {
        }

        public static Builder aBuilder() {
            return new Builder();
        }

        public Builder withValues(List<String> values) {
            this.values = values;
            return this;
        }

        public Builder withTargetTable(String targetTable) {
            this.targetTable = targetTable;
            return this;
        }

        public ClickhouseRequestBlank build() {
            return new ClickhouseRequestBlank(values, targetTable);
        }
    }

    @Override
    public String toString() {
        return "ClickhouseRequestBlank{" +
                "values=" + values +
                ", targetTable='" + targetTable + '\'' +
                ", attemptCounter=" + attemptCounter +
                '}';
    }
}
