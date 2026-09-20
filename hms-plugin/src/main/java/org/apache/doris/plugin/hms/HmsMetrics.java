// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.hms;

import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.ToLongFunction;

/** Expose the per-catalog cache counters through the FE metrics endpoint; no kernel change. */
final class HmsMetrics {
    private static final Logger LOG = LogManager.getLogger(HmsMetrics.class);

    private HmsMetrics() {
    }

    static void register(String catalog, HmsAuthorization authorization) {
        for (String cache : authorization.stats().keySet()) {
            gauge("hms_authorization_cache_hits", "HMS authorization cache hits", catalog, cache,
                    authorization, CacheStats::hitCount);
            gauge("hms_authorization_cache_misses", "HMS authorization cache misses", catalog, cache,
                    authorization, CacheStats::missCount);
            gauge("hms_authorization_cache_load_failures", "HMS authorization cache load failures", catalog,
                    cache, authorization, CacheStats::loadFailureCount);
            gauge("hms_authorization_cache_evictions", "HMS authorization cache evictions", catalog, cache,
                    authorization, CacheStats::evictionCount);
        }
        LOG.info("HMS authorization metrics registered for catalog {}", catalog);
    }

    private static void gauge(String name, String description, String catalog, String cache,
            HmsAuthorization authorization, ToLongFunction<CacheStats> value) {
        GaugeMetric<Long> metric = new GaugeMetric<Long>(name, MetricUnit.NOUNIT, description) {
            @Override
            public Long getValue() {
                return value.applyAsLong(authorization.stats().get(cache));
            }
        };
        metric.addLabel(new MetricLabel("catalog", catalog));
        metric.addLabel(new MetricLabel("cache", cache));
        // The registry keys by name and labels, so a rebuilt controller replaces its predecessor's gauge.
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
    }
}
