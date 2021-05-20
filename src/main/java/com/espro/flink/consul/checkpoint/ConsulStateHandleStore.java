/*
 * Copyright (c) SABIO GmbH, Hamburg 2021 - All rights reserved
 */
package com.espro.flink.consul.checkpoint;

import static java.util.Collections.emptyList;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the returned state handle to Consul.
 * <p>
 * Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles}, which in turn are written to Consul. This level of
 * indirection is necessary to keep the amount of data in Consul small. Consul is build for data to be not larger than 512 KB whereas state
 * can grow to multiple MBs.
 */
public class ConsulStateHandleStore<T extends Serializable> implements StateHandleStore<T, IntegerResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulStateHandleStore.class);

    private final ConsulClient client;

    private final RetrievableStateStorageHelper<T> storage;

    private final String checkpointBasePath;

    public ConsulStateHandleStore(ConsulClient client, RetrievableStateStorageHelper<T> storage, String checkpointBasePath) {
        this.client = client;
        this.storage = storage;
        this.checkpointBasePath = checkpointBasePath;
    }

    @Override
    public RetrievableStateHandle<T> addAndLock(String pathInConsul, T state) throws Exception {
        checkNotNull(pathInConsul, "Path in Consul");
        checkNotNull(state, "State");

        RetrievableStateHandle<T> storeHandle = storage.store(state);
        boolean success = false;

        try {
            byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);
            success = client.setKVBinaryValue(pathInConsul, serializedStoreHandle).getValue();
            return storeHandle;
        } finally {
            if (!success) {
                // cleanup if data was not stored in Consul
                if (storeHandle != null) {
                    storeHandle.discardState();
                }
            }
        }
    }

    @Override
    public void replace(String pathInConsul, IntegerResourceVersion resourceVersion, T state) throws Exception {
        checkNotNull(pathInConsul, "Path in Consul");
        checkNotNull(state, "State");

        RetrievableStateHandle<T> oldStateHandle = get(pathInConsul);
        RetrievableStateHandle<T> newStateHandle = storage.store(state);

        boolean success = false;

        try {
            byte[] serializedStoreHandle = InstantiationUtil.serializeObject(newStateHandle);
            success = client.setKVBinaryValue(pathInConsul, serializedStoreHandle).getValue();
        } finally {
            if (success) {
                oldStateHandle.discardState();
            } else {
                newStateHandle.discardState();
            }
        }
    }

    @Override
    public IntegerResourceVersion exists(String pathInConsul) throws Exception {
        checkNotNull(pathInConsul, "Path in Consul");

        GetBinaryValue binaryValue = client.getKVBinaryValue(pathInConsul).getValue();
        if (binaryValue != null) {
            return IntegerResourceVersion.valueOf((int) binaryValue.getModifyIndex());
        } else {
            return IntegerResourceVersion.notExisting();
        }
    }

    @Override
    public RetrievableStateHandle<T> getAndLock(String pathInConsul) throws Exception {
        checkNotNull(pathInConsul, "Path in Consul");
        return get(pathInConsul);
    }

    @Override
    public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
        List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

        List<GetBinaryValue> binaryValues = client.getKVBinaryValues(checkpointBasePath).getValue();
        if (binaryValues == null || binaryValues.isEmpty()) {
            LOG.debug("No state handles present in Consul for key prefix {}", checkpointBasePath);
            return Collections.emptyList();
        }

        for (GetBinaryValue binaryValue : binaryValues) {
            RetrievableStateHandle<T> stateHandle = InstantiationUtil.<RetrievableStateHandle<T>>deserializeObject(
                    binaryValue.getValue(),
                    Thread.currentThread().getContextClassLoader());

            stateHandles.add(new Tuple2<>(stateHandle, binaryValue.getKey()));
        }
        return stateHandles;
    }

    @Override
    public Collection<String> getAllHandles() throws Exception {
        List<String> keys = client.getKVKeysOnly(checkpointBasePath).getValue();
        if (keys != null) {
            return keys;
        }

        LOG.debug("No handles present in Consul for key prefix {}", checkpointBasePath);
        return emptyList();
    }

    @Override
    public boolean releaseAndTryRemove(String pathInConsul) throws Exception {
        checkNotNull(pathInConsul, "Path in Consul");

        RetrievableStateHandle<T> stateHandle = null;
        try {
            stateHandle = get(pathInConsul);
        } catch (Exception e) {
            LOG.warn("Could not retrieve the state handle from Consul {}.", pathInConsul, e);
        }

        try {
            client.deleteKVValue(pathInConsul);
            LOG.info("Value for key {} in Consul was deleted.", pathInConsul);
        } catch (Exception e) {
            LOG.info("Error while deleting state handle for checkpoint {}", pathInConsul, e);
            return false;
        }

        if (stateHandle != null) {
            stateHandle.discardState();
        }

        return true;
    }

    @Override
    public void releaseAndTryRemoveAll() throws Exception {
        Collection<String> checkpointPathsInConsul = getAllHandles();

        Exception exception = null;

        for (String pathInConsul : checkpointPathsInConsul) {
            try {
                releaseAndTryRemove(pathInConsul);
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw new Exception("Could not properly release and try removing all states.", exception);
        }
    }

    @Override
    public void clearEntries() throws Exception {
        Collection<String> checkpointPathsInConsul = getAllHandles();
        for (String pathInConsul : checkpointPathsInConsul) {
            client.deleteKVValue(pathInConsul);
            LOG.info("Value for key {} in Consul was deleted.", pathInConsul);
        }
    }

    @Override
    public void release(String pathInConsul) throws Exception {
        // There is no dedicated lock for a specific checkpoint
    }

    @Override
    public void releaseAll() throws Exception {
        // There are no locks
    }

    private RetrievableStateHandle<T> get(String path) {
        try {
            GetBinaryValue binaryValue = client.getKVBinaryValue(path).getValue();

            return InstantiationUtil.<RetrievableStateHandle<T>>deserializeObject(
                    binaryValue.getValue(),
                    Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
