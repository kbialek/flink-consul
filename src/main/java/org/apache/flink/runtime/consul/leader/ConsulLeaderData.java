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
package org.apache.flink.runtime.consul.leader;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.flink.util.Preconditions;

import java.io.*;
import java.util.UUID;

final class ConsulLeaderData {

	private final String address;
	private final UUID sessionId;

	public ConsulLeaderData(String address, UUID sessionId) {
		this.address = Preconditions.checkNotNull(address, "address");
		this.sessionId = Preconditions.checkNotNull(sessionId, "sessionId");
	}

	public static ConsulLeaderData from(String address, UUID sessionId) {
		return new ConsulLeaderData(address, sessionId);
	}

	public static ConsulLeaderData from(byte[] bytes) {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(is);
			String address = ois.readUTF();
			UUID sessionId = (UUID) ois.readObject();
			return new ConsulLeaderData(address, sessionId);
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException("ConsulLeaderData deserialization failure", e);
		}
	}

	public byte[] toBytes() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeUTF(address);
			oos.writeObject(sessionId);
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalStateException("ConsulLeaderData serialization failure", e);
		}
	}

	public String getAddress() {
		return address;
	}

	public UUID getSessionId() {
		return sessionId;
	}

	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
	}
}
