package org.project.chronos.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class WebscraperMetrics extends AbstractMetrics {

    public WebscraperMetrics(MeterRegistry meterRegistry) {
        super(meterRegistry);
    }

}
