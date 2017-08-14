/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.test;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.impl.util.OutboxImpl;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.function.Function;

import static com.hazelcast.util.Preconditions.checkNotNegative;

/**
 * Implements {@code Outbox} with an array of {@link ArrayDeque}s.
 */
public final class TestOutbox implements Outbox {

    private static final SerializationService IDENTITY_SERIALIZER = new MockSerializationService();

    private final Queue<Object>[] buckets;
    private final Queue<Entry<Data, Data>> snapshotQueue = new ArrayDeque<>();
    private final OutboxImpl outbox;

    /**
     * @param capacities Capacities of individual buckets. Number of buckets
     *                   is determined by the number of provided capacities.
     *                   There is no snapshot bucket.
     */
    public TestOutbox(int ... capacities) {
        this(capacities, 0);
    }

    /**
     * @param edgeCapacities Capacities of individual buckets. Number of buckets
     *                      is determined by the number of provided capacities.
     * @param snapshotCapacity Capacity of snapshot bucket. If 0, snapshot queue
     *                         is not present.
     */
    public TestOutbox(int[] edgeCapacities, int snapshotCapacity) {
        checkNotNegative(snapshotCapacity, "snapshotCapacity must be >= 0 (0 for no snapshot queue)");

        buckets = new Queue[edgeCapacities.length];
        Arrays.setAll(buckets, i -> new ArrayDeque());

        Function<Object, ProgressState>[] outstreams = new Function[edgeCapacities.length + snapshotCapacity > 0 ? 1 : 0];
        Arrays.setAll(outstreams, i ->
                i < edgeCapacities.length
                    ? e -> addToQueue(buckets[i], edgeCapacities[i], e)
                    : e -> addToQueue(snapshotQueue, snapshotCapacity, (Entry<Data, Data>) e));

        outbox = new OutboxImpl(outstreams, snapshotCapacity  > 0, new ProgressTracker(), IDENTITY_SERIALIZER);
    }

    private static <E> ProgressState addToQueue(Queue<? super E> queue, int capacity, E o) {
        if (capacity > queue.size()) {
            queue.offer(o);
            return ProgressState.DONE;
        } else {
            return ProgressState.NO_PROGRESS;
        }
    }

    @Override
    public int bucketCount() {
        return outbox.bucketCount();
    }

    @Override
    public boolean offer(int ordinal, @Nonnull Object item) {
        return outbox.offer(ordinal, item);
    }

    @Override
    public boolean offer(int[] ordinals, @Nonnull Object item) {
        return outbox.offer(ordinals, item);
    }

    @Override
    public boolean offer(@Nonnull Object item) {
        return outbox.offer(item);
    }

    @Override
    public boolean offerSnapshot(Object key, Object value) {
        return outbox.offerSnapshot(key, value);
    }

    @Override
    public String toString() {
        return Arrays.toString(buckets);
    }

    /**
     * Exposes individual buckets to the testing code.
     * @param ordinal ordinal of the bucket
     */
    public Queue<Object> queueWithOrdinal(int ordinal) {
        return buckets[ordinal];
    }

    private static class MockSerializationService implements SerializationService {

        @Override
        public <B extends Data> B toData(Object obj) {
            return (B) new MockData(obj);
        }

        @Override
        public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
            return (B) new MockData(obj);
        }

        @Override
        public <T> T toObject(Object data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T toObject(Object data, Class klazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManagedContext getManagedContext() {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockData implements Data {
        private final Object object;

        public MockData(Object object) {
            this.object = object;
        }

        public Object getObject() {
            return object;
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int totalSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyTo(byte[] dest, int destPos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int dataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getHeapCost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash64() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPortable() {
            throw new UnsupportedOperationException();
        }
    }
}
