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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Runner responsible for executing a {@link Dispatcher} or a subclass thereof.
 */
public class DispatcherRunnerImpl<T extends Dispatcher> implements DispatcherRunner {

	@Nonnull
	private final T dispatcher;

	public DispatcherRunnerImpl(
			@Nonnull DispatcherFactory<T> dispatcherFactory,
			@Nonnull RpcService rpcService,
			@Nonnull DispatcherFactoryServices dispatcherFactoryServices) throws Exception {
		this.dispatcher = dispatcherFactory.createDispatcher(
			rpcService,
			dispatcherFactoryServices);
		dispatcher.start();
	}

	@Nullable
	@Override
	public T getDispatcher() {
		return dispatcher;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		Exception exception = null;

		Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);

		terminationFutures.add(dispatcher.closeAsync());

		if (exception != null) {
			terminationFutures.add(FutureUtils.completedExceptionally(exception));
		}

		return FutureUtils.completeAll(terminationFutures);
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return dispatcher.getTerminationFuture();
	}
}
