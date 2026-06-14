package org.project.chronos.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.project.chronos.constants.MetricConstants.counterPostfix;
import static org.project.chronos.constants.MetricConstants.timerPostFix;

public abstract class AbstractMetrics {

    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();

    public AbstractMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordTime(String metricPrefix, String desc, Long startTime, String... additionalCustomTags) {

        long timeTaken = System.currentTimeMillis() - startTime;
        String[] additionalTags = processTags(additionalCustomTags);
        String timerName = metricPrefix.concat(timerPostFix);
        Timer timer = timerCache.computeIfAbsent(timerName, name ->
                Timer.builder(name)
                        .tags(additionalTags)
                        .description(desc)
                        .register(this.meterRegistry)
        );

        timer.record(timeTaken, TimeUnit.MILLISECONDS);
    }

    public void count(String metricPrefix, String desc, String... tags) {

        String[] additionalTags = processTags(tags);
        String counterName = metricPrefix.concat(counterPostfix);
        Counter counter = counterCache.computeIfAbsent(counterName, name ->
                Counter.builder(name)
                        .tags(additionalTags)
                        .description(desc)
                        .register(this.meterRegistry)
        );

        counter.increment();
    }

    private static String[] processTags(String[] customTags) {

        if (customTags != null && customTags.length != 0) {
            List<String> tagsList = new ArrayList<>();

            for (String tag : customTags) {
                String[] keyValue = tag.split(":", 2);
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Invalid tag format: " + tag);
                }

                tagsList.add(keyValue[0]);
                tagsList.add(keyValue[1]);
            }

            return tagsList.toArray(new String[0]);
        } else {
            return new String[0];
        }
    }
}
