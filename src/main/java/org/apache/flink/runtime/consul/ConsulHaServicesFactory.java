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
package org.apache.flink.runtime.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;

import java.util.concurrent.Executor;

public class ConsulHaServicesFactory implements HighAvailabilityServicesFactory {

    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {
        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

        ConsulClient consulClient = new ConsulClient("localhost");
        return new ConsulHaServices(consulClient,
                executor,
                configuration,
                blobStoreService);
    }
}
