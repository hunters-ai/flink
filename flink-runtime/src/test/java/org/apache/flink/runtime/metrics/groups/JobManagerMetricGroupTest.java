/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.fail;

/**
 * Tests for the {@link JobManagerMetricGroup}.
 */
public class JobManagerMetricGroupTest extends TestLogger {

	private MetricRegistryImpl metricRegistry;

	private TestingReporter reporter;

	@Before
	public void setup() {
		this.reporter = new TestingReporter();
		this.metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.defaultMetricRegistryConfiguration(),
			Collections.singleton(ReporterSetup.forReporter("testingReporter", reporter)));
	}

	@After
	public void teardown() throws Exception {
		if (metricRegistry != null) {
			metricRegistry.shutdown().get();
			metricRegistry = null;
		}
	}

	@Test
	public void addMetric_existingMetricWithNonResettableName_shouldKeepOldMetric() {
		final String nonResettableMetricName = "foobar";

		runJobManagerMetricGroupAddMetricTest(nonResettableMetricName, MetricPrecedence.FIRST_METRIC);
	}

	@Test
	public void addMetric_existingMetricWithResettableName_shouldReplaceOldMetric() {
		final String resettableMetricName = MetricNames.NUM_RUNNING_JOBS;

		runJobManagerMetricGroupAddMetricTest(resettableMetricName, MetricPrecedence.LAST_METRIC);
	}

	private void runJobManagerMetricGroupAddMetricTest(String metricName, MetricPrecedence precedence) {
		final JobManagerMetricGroup jobManagerMetricGroup = createJobManagerMetricGroup();
		final Gauge<Integer> firstGauge = () -> 1;
		final Gauge<Integer> secondGauge = () -> 2;

		final Gauge<Integer> expectedGauge;

		if (precedence == MetricPrecedence.FIRST_METRIC) {
			expectedGauge = firstGauge;
		} else {
			expectedGauge = secondGauge;
		}

		try {
			jobManagerMetricGroup.gauge(metricName, firstGauge);
			jobManagerMetricGroup.gauge(metricName, secondGauge);

			final Gauge<Integer> gauge = reporter.<Integer>getGauge(metricName)
				.orElseThrow(() -> new AssertionError(
					String.format("Gauge %s has not been registered.", metricName)));

			if (gauge != expectedGauge) {
				fail("The returned gauge does not equal the expected gauge.");
			}
		} finally {
			jobManagerMetricGroup.close();
		}
	}

	private JobManagerMetricGroup createJobManagerMetricGroup() {
		return new JobManagerMetricGroup(
			metricRegistry,
			"localhost");
	}

	private enum MetricPrecedence {
		FIRST_METRIC,
		LAST_METRIC,
	}

	private static class TestingReporter implements MetricReporter {
		private final Map<String, Gauge<?>> gauges;

		private TestingReporter() {
			gauges = new HashMap<>();
		}

		@Override
		public void open(MetricConfig config) {}

		@Override
		public void close() {}

		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			if (metric instanceof Gauge) {
				gauges.put(metricName, ((Gauge) metric));
			}
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {}

		<T> Optional<Gauge<T>> getGauge(String metricName) {
			@SuppressWarnings("unchecked")
			final Gauge<T> value = (Gauge<T>) gauges.get(metricName);

			return Optional.ofNullable(value);
		}
	}
}
