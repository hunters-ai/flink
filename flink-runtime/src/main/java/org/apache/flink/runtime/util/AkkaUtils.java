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

package org.apache.flink.runtime.util;

import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.guava18.com.google.common.net.HostAndPort;

import akka.actor.Address;

import java.net.MalformedURLException;

/**
 * Utility class for Akka convenience functions. Java equivalent of {@link AkkaUtils}.
 */
public class AkkaUtils {

	public static HostAndPort extractHostAndPortFromAkkaURL(String address) throws FlinkException {
		final Address akkaAddress;
		// this only works as long as the address is Akka based
		try {
			akkaAddress = org.apache.flink.runtime.akka.AkkaUtils.getAddressFromAkkaURL(address);
		} catch (MalformedURLException e) {
			throw new FlinkException("Could not extract the hostname from the given address \'" +
				address + "\'.", e);
		}

		final String hostname;
		if (akkaAddress.host().isDefined()) {
			hostname = akkaAddress.host().get();
		} else {
			hostname = "localhost";
		}

		final int port;
		if (akkaAddress.port().isDefined()) {
			port = (int) akkaAddress.port().get();
		} else {
			port = -1;
		}

		return HostAndPort.fromParts(hostname, port);
	}
}
