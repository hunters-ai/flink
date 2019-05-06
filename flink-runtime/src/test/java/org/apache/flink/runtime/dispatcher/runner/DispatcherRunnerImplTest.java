/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactoryServices;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.mapreduce.Job;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for the {@link DispatcherRunnerImpl}.
 */
public class DispatcherRunnerImplTest extends TestLogger {

	private static final Time TESTING_TIMEOUT = Time.seconds(10L);

	@ClassRule
	public static BlobServerResource blobServerResource = new BlobServerResource();

	/**
	 * See FLINK-11665.
	 */
	@Test
	public void testResourceCleanupUnderLeadershipChange() throws Exception {
		final TestingRpcService rpcService = new TestingRpcService();
		final TestingLeaderElectionService dispatcherLeaderElectionService = new TestingLeaderElectionService();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServicesBuilder()
			.setDispatcherLeaderElectionService(dispatcherLeaderElectionService)
			.build();

		final Configuration configuration = new Configuration();
		final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

		final DispatcherFactoryServices dispatcherFactoryServices = new DispatcherFactoryServices(
			configuration,
			highAvailabilityServices,
			() -> new CompletableFuture<>(),
			blobServerResource.getBlobServer(),
			new TestingHeartbeatServices(),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			new MemoryArchivedExecutionGraphStore(),
			fatalErrorHandler,
			VoidHistoryServerArchivist.INSTANCE,
			null);

		final JobGraph jobGraph = createJobGraphWithBlobs();

		try (final DispatcherRunnerImpl dispatcherRunner = new DispatcherRunnerImpl(SessionDispatcherFactory.INSTANCE, rpcService, dispatcherFactoryServices)) {
			// initial run
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

			final Dispatcher dispatcher = dispatcherRunner.getDispatcher();

			DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			dispatcherGateway.submitJob(jobGraph, TESTING_TIMEOUT).get();

			dispatcherLeaderElectionService.notLeader();

			// recovering submitted jobs
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

			dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			// cancellation of the job should remove everything
			dispatcherGateway.cancelJob(jobGraph.getJobID(), TESTING_TIMEOUT).get();

			dispatcherLeaderElectionService.notLeader();

			// regranting leadership to 
		} finally {
			rpcService.stopService().join();

			fatalErrorHandler.rethrowError();
		}
	}

	private JobGraph createJobGraphWithBlobs() {
		final JobVertex vertex = new JobVertex("test vertex");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(1);

		final JobGraph jobGraph = new JobGraph("Test job graph", vertex);
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

}
