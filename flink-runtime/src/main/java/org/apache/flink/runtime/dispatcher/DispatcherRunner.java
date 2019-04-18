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

import org.apache.flink.util.AutoCloseableAsync;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Base interface for all {@link Dispatcher} runner implementations.
 */
public interface DispatcherRunner extends AutoCloseableAsync {

	/**
	 * Get the currently running {@link Dispatcher}. Can be null
	 * if none is running at the moment.
	 *
	 * @return the currently running dispatcher or null if none is running
	 */
	@Nullable
	Dispatcher getDispatcher();

	/**
	 * Return the termination future of this runner. The termination future
	 * is being completed, once the runner has been completely terminated.
	 *
	 * @return termination future of this runner
	 */
	CompletableFuture<Void> getTerminationFuture();
}
