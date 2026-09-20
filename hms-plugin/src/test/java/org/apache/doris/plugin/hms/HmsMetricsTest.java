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

import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class HmsMetricsTest {
    private static List<Metric> metrics(String name, String catalog, String cache) {
        List<Metric> result = new ArrayList<>();
        for (Metric<?> metric : MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(name)) {
            List<String> values = new ArrayList<>();
            for (MetricLabel label : metric.getLabels()) {
                values.add(label.getValue());
            }
            if (values.equals(Arrays.asList(catalog, cache))) {
                result.add(metric);
            }
        }
        return result;
    }

    @Test
    void gaugesFollowTheRegisteredCachesAndReplaceEarlierInstancesOfTheSameCatalog() {
        HmsPluginConfig config = HmsAuthorizationTest.config(600);
        HmsAuthorizationTest.Source source = new HmsAuthorizationTest.Source();
        HmsAuthorization first = new HmsAuthorization(config, source);
        HmsMetrics.register("hms_metrics_test", first);
        first.table(HmsAuthorizationTest.TABLE);
        first.table(HmsAuthorizationTest.TABLE);
        List<Metric> hits = metrics("hms_authorization_cache_hits", "hms_metrics_test", "tables");
        Assertions.assertEquals(1, hits.size());
        Assertions.assertEquals(1L, hits.get(0).getValue());
        Assertions.assertEquals(1L, metrics("hms_authorization_cache_misses", "hms_metrics_test", "tables")
                .get(0).getValue());
        Assertions.assertEquals(0L, metrics("hms_authorization_cache_load_failures", "hms_metrics_test", "tables")
                .get(0).getValue());
        source.fail = true;
        HmsAuthorization second = new HmsAuthorization(config, source);
        HmsMetrics.register("hms_metrics_test", second);
        Assertions.assertThrows(HmsAuthorizationException.class, () -> second.table(HmsAuthorizationTest.TABLE));
        // ALTER CATALOG rebuilds the controller; the gauges must read the live instance, not the old one.
        Assertions.assertEquals(1, metrics("hms_authorization_cache_hits", "hms_metrics_test", "tables").size());
        Assertions.assertEquals(0L, metrics("hms_authorization_cache_hits", "hms_metrics_test", "tables")
                .get(0).getValue());
        Assertions.assertEquals(1L, metrics("hms_authorization_cache_load_failures", "hms_metrics_test", "tables")
                .get(0).getValue());
        Assertions.assertEquals(1, metrics("hms_authorization_cache_evictions", "hms_metrics_test", "roles").size());
        Assertions.assertEquals(1, metrics("hms_authorization_cache_misses", "hms_metrics_test", "databases").size());
    }
}
