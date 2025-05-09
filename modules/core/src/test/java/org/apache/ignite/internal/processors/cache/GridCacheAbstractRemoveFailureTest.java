/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests that removes are not lost when topology changes.
 */
public abstract class GridCacheAbstractRemoveFailureTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final int CLI_IDX = 42;

    /** Keys count. */
    private static final int KEYS_CNT = 10_000;

    /** Test duration. */
    private static final long DUR = 90 * 1000L;

    /** Cache data assert frequency. */
    private static final long ASSERT_FREQ = 10_000;

    /** Kill delay. */
    private static final T2<Integer, Integer> KILL_DELAY = new T2<>(2000, 5000);

    /** Start delay. */
    private static final T2<Integer, Integer> START_DELAY = new T2<>(2000, 5000);

    /** */
    private static String sizePropVal;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(testClientNode() && getTestIgniteInstanceName(CLI_IDX).equals(igniteInstanceName));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Need to increase value set in GridAbstractTest
        sizePropVal = System.getProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE);

        System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "100000");

        startGrids(GRID_CNT);

        if (testClientNode())
            startClientGrid(CLI_IDX);
        else
            startGrid(CLI_IDX);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, sizePropVal != null ? sizePropVal : "");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DUR + 60_000;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Near cache configuration.
     */
    protected abstract NearCacheConfiguration nearCache();

    /**
     * @return {@code True} if test updates from client node.
     */
    protected boolean testClientNode() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndRemove() throws Exception {
        putAndRemove(duration(), null, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndRemovePessimisticTx() throws Exception {
        if (atomicityMode() != CacheAtomicityMode.TRANSACTIONAL)
            return;

        putAndRemove(duration(), PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndRemoveOptimisticSerializableTx() throws Exception {
        if (atomicityMode() != CacheAtomicityMode.TRANSACTIONAL)
            return;

        putAndRemove(duration(), OPTIMISTIC, SERIALIZABLE);
    }

    /** */
    protected long duration() {
        return DUR;
    }

    /**
     * @param duration Test duration.
     * @param txConcurrency Transaction concurrency if test explicit transaction.
     * @param txIsolation Transaction isolation if test explicit transaction.
     * @throws Exception If failed.
     */
    private void putAndRemove(long duration,
        final TransactionConcurrency txConcurrency,
        final TransactionIsolation txIsolation) throws Exception {
        assertEquals(testClientNode(), (boolean)grid(CLI_IDX).configuration().isClientMode());

        grid(CLI_IDX).destroyCache(DEFAULT_CACHE_NAME);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setCacheMode(cacheMode());

        if (cacheMode() == PARTITIONED)
            ccfg.setBackups(1);

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setNearConfiguration(nearCache());

        final IgniteCache<Integer, Integer> sndCache0 = grid(CLI_IDX).createCache(ccfg);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicLong cntr = new AtomicLong();

        final AtomicLong errCntr = new AtomicLong();

        // Expected values in cache.
        final Map<Integer, GridTuple<Integer>> expVals = new ConcurrentHashMap<>();

        final AtomicReference<CyclicBarrier> cmp = new AtomicReference<>();

        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("update-thread");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                IgniteTransactions txs = sndCache0.unwrap(Ignite.class).transactions();

                while (!stop.get()) {
                    for (int i = 0; i < 100; i++) {
                        int key = rnd.nextInt(KEYS_CNT);

                        boolean put = rnd.nextInt(0, 100) > 10;

                        while (true) {
                            try {
                                if (put) {
                                    boolean failed = false;

                                    if (txConcurrency != null) {
                                        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
                                            sndCache0.put(key, i);

                                            tx.commit();
                                        }
                                        catch (CacheException | IgniteException e) {
                                            if (!X.hasCause(e, ClusterTopologyCheckedException.class)) {
                                                log.error("Unexpected error: " + e);

                                                throw e;
                                            }

                                            failed = true;
                                        }
                                    }
                                    else
                                        sndCache0.put(key, i);

                                    if (!failed)
                                        expVals.put(key, F.t(i));
                                }
                                else {
                                    boolean failed = false;

                                    if (txConcurrency != null) {
                                        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
                                            sndCache0.remove(key);

                                            tx.commit();
                                        }
                                        catch (CacheException | IgniteException e) {
                                            if (!X.hasCause(e, ClusterTopologyCheckedException.class)) {
                                                log.error("Unexpected error: " + e);

                                                throw e;
                                            }

                                            failed = true;
                                        }
                                    }
                                    else
                                        sndCache0.remove(key);

                                    if (!failed)
                                        expVals.put(key, F.<Integer>t(null));
                                }

                                break;
                            }
                            catch (CacheException e) {
                                if (put)
                                    log.error("Put failed [key=" + key + ", val=" + i + ']', e);
                                else
                                    log.error("Remove failed [key=" + key + ']', e);

                                errCntr.incrementAndGet();
                            }
                        }
                    }

                    cntr.addAndGet(100);

                    CyclicBarrier barrier = cmp.get();

                    if (barrier != null) {
                        log.info("Wait data check.");

                        barrier.await(60_000, TimeUnit.MILLISECONDS);

                        log.info("Finished wait data check.");
                    }
                }

                return null;
            }
        });

        IgniteInternalFuture killFut = createAndRunConcurrentAction(stop, cmp);

        try {
            long stopTime = duration + U.currentTimeMillis();

            long nextAssert = U.currentTimeMillis() + ASSERT_FREQ;

            while (U.currentTimeMillis() < stopTime) {
                long start = System.nanoTime();

                long ops = cntr.longValue();

                U.sleep(1000);

                long diff = cntr.longValue() - ops;

                double time = (System.nanoTime() - start) / 1_000_000_000d;

                long opsPerSecond = (long)(diff / time);

                log.info("Operations/second: " + opsPerSecond);

                if (U.currentTimeMillis() >= nextAssert) {
                    CyclicBarrier barrier = new CyclicBarrier(3, new Runnable() {
                        @Override public void run() {
                            try {
                                cmp.set(null);

                                log.info("Checking cache content.");

                                assertCacheContent(expVals);

                                log.info("Finished check cache content.");
                            }
                            catch (Throwable e) {
                                log.error("Unexpected error: " + e, e);

                                throw e;
                            }
                        }
                    });

                    log.info("Start cache content check.");

                    cmp.set(barrier);

                    try {
                        barrier.await(60_000, TimeUnit.MILLISECONDS);
                    }
                    catch (TimeoutException e) {
                        U.dumpThreads(log);

                        fail("Failed to check cache content: " + e);
                    }

                    log.info("Cache content check done.");

                    nextAssert = System.currentTimeMillis() + ASSERT_FREQ;
                }
            }
        }
        finally {
            stop.set(true);
        }

        killFut.get();

        updateFut.get();

        log.info("Test finished. Update errors: " + errCntr.get());
    }

    /** */
    protected IgniteInternalFuture createAndRunConcurrentAction(final AtomicBoolean stop, final AtomicReference<CyclicBarrier> cmp) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                while (!stop.get()) {
                    U.sleep(random(KILL_DELAY.get1(), KILL_DELAY.get2()));

                    killAndRestart(stop, random(1, GRID_CNT + 1));

                    CyclicBarrier barrier = cmp.get();

                    if (barrier != null) {
                        log.info("Wait data check.");

                        barrier.await(60_000, TimeUnit.MILLISECONDS);

                        log.info("Finished wait data check.");
                    }
                }

                return null;
            }
        });
    }

    /**
     * @param stop Stop flag.
     * @throws Exception If failed.
     */
    protected void killAndRestart(AtomicBoolean stop, int nodeIdx) throws Exception {
        if (stop.get())
            return;

        log.info("Killing node " + nodeIdx);

        stopGrid(nodeIdx);

        U.sleep(random(START_DELAY.get1(), START_DELAY.get2()));

        log.info("Restarting node " + nodeIdx);

        startGrid(nodeIdx);

        if (stop.get())
            return;

        U.sleep(1000);
    }

    /**
     * @param expVals Expected values in cache.
     */
    private void assertCacheContent(Map<Integer, GridTuple<Integer>> expVals) {
        assert !expVals.isEmpty();

        Collection<Integer> failedKeys = new HashSet<>();

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = grid(i);

            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            for (Map.Entry<Integer, GridTuple<Integer>> expVal : expVals.entrySet()) {
                Integer val = cache.get(expVal.getKey());

                if (!Objects.equals(expVal.getValue().get(), val)) {
                    failedKeys.add(expVal.getKey());

                    boolean primary = affinity(cache).isPrimary(ignite.cluster().localNode(), expVal.getKey());
                    boolean backup = affinity(cache).isBackup(ignite.cluster().localNode(), expVal.getKey());

                    log.error("Unexpected cache data [exp=" + expVal +
                        ", actual=" + val +
                        ", nodePrimary=" + primary +
                        ", nodeBackup=" + backup +
                        ", nodeIdx" + i +
                        ", nodeId=" + ignite.cluster().localNode().id() + ']');
                }
            }
        }

        assertTrue("Unexpected data for keys: " + failedKeys, failedKeys.isEmpty());
    }

    /**
     * @param min Min possible value.
     * @param max Max possible value (exclusive).
     * @return Random value.
     */
    protected static int random(int min, int max) {
        if (max == min)
            return max;

        return ThreadLocalRandom.current().nextInt(min, max);
    }
}
