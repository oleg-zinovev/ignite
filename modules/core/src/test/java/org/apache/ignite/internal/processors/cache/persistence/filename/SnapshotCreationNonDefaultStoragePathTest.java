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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test snapshot can be created when {@link DataStorageConfiguration#setStoragePath(String)} used.
 */
public class SnapshotCreationNonDefaultStoragePathTest extends AbstractDataRegionRelativeStoragePathTest {
    /** {@inheritDoc} */
    @Override protected DataStorageConfiguration dataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setStoragePath(storagePath(STORAGE_PATH))
            .setExtraStoragePaths(storagePath(STORAGE_PATH_2), storagePath(IDX_PATH));
    }

    /** {@inheritDoc} */
    @Override CacheConfiguration[] ccfgs() {
        return new CacheConfiguration[] {
            ccfg("cache0", null, storagePaths(STORAGE_PATH_2, STORAGE_PATH))
        };
    }

    /** */
    @Test
    public void testSnapshotThrowsIfExtraRootExists() throws Exception {
        IgniteEx srv = startAndActivate();

        putData();

        checkDataExists();

        String snpName = "mysnp";

        File srvExtraSnpRoot = new SnapshotFileTree(srv.context(), snpName, null).extraStorages().get(storagePath(STORAGE_PATH_2));

        assertTrue(srvExtraSnpRoot.mkdirs());

        assertThrowsWithCause(() -> srv.snapshot().createSnapshot(snpName).get(), IgniteException.class);

        for (Ignite node : G.allGrids()) {
            SnapshotFileTree sft = new SnapshotFileTree(((IgniteEx)node).context(), snpName, null);

            assertTrue(sft.nodeStorage().getAbsolutePath() + " must not extists", !sft.nodeStorage().exists());

            for (File es : sft.extraStorages().values()) {
                assertTrue(es.getAbsolutePath() + " must not extists", !es.exists());
            }
        }

        U.delete(srvExtraSnpRoot);

        srv.snapshot().createSnapshot(snpName).get();

        restoreAndCheck(snpName, null);
    }

    /** */
    @Test
    public void testSnapshotCanBeCreated() throws Exception {
        IgniteEx srv = startAndActivate();

        putData();

        checkDataExists();

        srv.snapshot().createSnapshot("mysnp").get();

        File fullPathSnp = new File(U.defaultWorkDirectory(), SNP_PATH);

        srv.context().cache().context().snapshotMgr().createSnapshot("mysnp2", fullPathSnp.getAbsolutePath(), false, false).get();

        restoreAndCheck("mysnp", null);
        restoreAndCheck("mysnp2", fullPathSnp.getAbsolutePath());
    }

    /** */
    @Test
    public void testRestoreOnSmallerTopology() throws Exception {
        IgniteEx srv = startAndActivate();

        putData();

        checkDataExists();

        srv.snapshot().createSnapshot("mysnp").get();

        File fullPathSnp = new File(U.defaultWorkDirectory(), SNP_PATH);

        srv.context().cache().context().snapshotMgr().createSnapshot("mysnp2", fullPathSnp.getAbsolutePath(), false, false).get();

        String grid1ConsId = consId(grid(1).configuration());

        stopGrid(1);

        resetBaselineTopology();

        BiConsumer<String, String> check = (name, path) -> {
            for (CacheConfiguration<?, ?> ccfg : ccfgs())
                grid(0).destroyCache(ccfg.getName());

            try {
                awaitPartitionMapExchange();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            srv.context().cache().context().snapshotMgr().restoreSnapshot(name, path, null).get();

            checkDataExists();
        };

        if (pathMode == PathMode.SEPARATE_ROOT) {
            // Must move snapshot file from grid1 to other so it can be restored.
            assertThrowsAnyCause(
                log,
                () -> {
                    check.accept("mysnp", null);
                    return null;
                },
                IgniteException.class,
                "No snapshot metadatas found for the baseline nodes with consistent ids: "
            );

            Path[] copyStoppedNodeData = new Path[] {
                Path.of(DFLT_SNAPSHOT_DIRECTORY, "mysnp"),
                Path.of(STORAGE_PATH_2, DFLT_SNAPSHOT_DIRECTORY, "mysnp"),
                Path.of(IDX_PATH, DFLT_SNAPSHOT_DIRECTORY, "mysnp")
            };

            for (Path copyPath : copyStoppedNodeData) {
                FileUtils.copyDirectory(
                    Path.of(U.defaultWorkDirectory(), grid1ConsId, copyPath.toString()).toFile(),
                    Path.of(U.defaultWorkDirectory(), consId(grid(0).configuration()), copyPath.toString()).toFile()
                );
            }
        }

        check.accept("mysnp", null);
        check.accept("mysnp2", fullPathSnp.getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override void checkFileTrees(List<NodeFileTree> fts) {
        for (NodeFileTree ft : fts) {
            for (CacheConfiguration<?, ?> ccfg : ccfgs()) {
                assertTrue(!severalCacheStorages || ccfg.getStoragePaths().length > 1);

                for (String cs : ccfg.getStoragePaths()) {

                    File customRoot = ensureExists(pathMode == PathMode.ABS
                        ? new File(cs)
                        : new File(ft.root(), cs)
                    );

                    String foldeNamePath = (cs.equals(storagePath(STORAGE_PATH)) ? "" : "db/") + ft.folderName();

                    File nodeStorage = ensureExists(new File(customRoot, foldeNamePath));

                    ensureExists(new File(nodeStorage, ft.defaultCacheStorage(ccfg).getName()));
                }
            }
        }
    }
}
