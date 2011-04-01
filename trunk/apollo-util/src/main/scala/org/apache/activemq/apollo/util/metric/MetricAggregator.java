/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.util.metric;

import java.util.ArrayList;

public class MetricAggregator extends Metric {

    ArrayList<Metric> metrics = new ArrayList<Metric>();

    public MetricAggregator name(String name) {
        return (MetricAggregator) super.name(name);
    }

    public MetricAggregator unit(String unit) {
        return (MetricAggregator) super.unit(unit);
    }

    public void add(Metric metric) {
        metrics.add(metric);
        if (getUnit() != null) {
            metric.setUnit(getUnit());
        }
    }

    public boolean remove(Metric metric) {
        return metrics.remove(metric);
    }

    public void removeAllMetrics() {
        metrics.clear();
    }

    public Float average() {
        if (metrics.isEmpty()) {
            return null;
        }
        long rc = 0;
        int count = 0;
        for (Metric metric : metrics) {
            rc += metric.counter();
            count++;
        }
        return rc * 1.0f / count;
    }

    public Float deviation() {
        if (metrics.isEmpty()) {
            return null;
        }
        long values[] = new long[metrics.size()];

        long sum=0;
        for (int i=0; i < values.length; i++) {
            values[i] = metrics.get(i).counter();
            sum += values[i];
        }

        double mean = (1.0 * sum) / values.length;
        double rc = 0;
        for (long value : values) {
            double v = value - mean;
            rc += (v*v);
        }
        return (float)Math.sqrt(rc / values.length);
    }

    public Float total(Period p) {
        return p.rate(total());
    }

    public long total() {
        long rc = 0;
        for (Metric metric : metrics) {
            rc += metric.counter();
        }
        return rc;
    }

    public Long min() {
        if (metrics.isEmpty()) {
            return null;
        }
        long rc = Long.MAX_VALUE;
        for (Metric metric : metrics) {
            long t = metric.counter();
            if (t < rc) {
                rc = t;
            }
        }
        return rc;
    }

    public Long max() {
        if (metrics.isEmpty()) {
            return null;
        }
        long rc = Long.MIN_VALUE;
        for (Metric metric : metrics) {
            long t = metric.counter();
            if (t > rc) {
                rc = t;
            }
        }
        return rc;
    }

    @Override
    public long counter() {
        return total();
    }

    public String getRateSummary(Period period) {
        return String
                .format("%s: total=%(,.2f, avg=%(,.2f, min=%(,.2f, max=%(,.2f in %s/s", getName(), period.rate(total()), period.rate(average()), period.rate(min()), period.rate(max()), getUnit());
    }

    public String getChildRateSummary(Period period) {
        StringBuilder rc = new StringBuilder();
        rc.append("{\n");
        for (Metric metric : metrics) {
            rc.append("  ");
            rc.append(metric.getRateSummary(period));
            rc.append("\n");
        }
        rc.append("}");
        return rc.toString();
    }

    @Override
    public long reset() {
        long rc = 0;
        for (Metric metric : metrics) {
            rc += metric.reset();
        }
        return rc;
    }

}
