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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Get request. Responsible for obtaining entry from primary node. 'Near' means 'Initiating node' here, not 'Near Cache'.
 */
public class GridNearGetRequest extends GridCacheIdMessage implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final int READ_THROUGH_FLAG_MASK = 0x01;

    /** */
    private static final int SKIP_VALS_FLAG_MASK = 0x02;

    /** */
    private static final int ADD_READER_FLAG_MASK = 0x04;

    /** */
    public static final int RECOVERY_FLAG_MASK = 0x08;

    /** Future ID. */
    private IgniteUuid futId;

    /** Sub ID. */
    private IgniteUuid miniId;

    /** Version. */
    private GridCacheVersion ver;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private LinkedHashMap<KeyCacheObject, Boolean> keyMap;

    /** */
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** */
    @GridDirectCollection(boolean.class)
    private List<Boolean> readersFlags;

    /** */
    private byte flags;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Task name hash. */
    private int taskNameHash;

    /** TTL for read operation. */
    private long createTtl;

    /** TTL for read operation. */
    private long accessTtl;

    /** Transaction label. */
    private @Nullable String txLbl;

    /**
     * Empty constructor.
     */
    public GridNearGetRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param skipVals Skip values flag. When false, only boolean values will be returned indicating whether
     *      cache entry has a value.
     * @param topVer Topology version.
     * @param taskNameHash Task name hash.
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     * @param addDepInfo Deployment info.
     * @param txLbl Transaction label.
     */
    public GridNearGetRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        Map<KeyCacheObject, Boolean> keys,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean addReader,
        boolean skipVals,
        boolean addDepInfo,
        boolean recovery,
        @Nullable String txLbl
    ) {
        assert futId != null;
        assert miniId != null;
        assert keys != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;

        this.keys = new ArrayList<>(keys.size());

        if (addReader)
            readersFlags = new ArrayList<>(keys.size());

        for (Map.Entry<KeyCacheObject, Boolean> entry : keys.entrySet()) {
            this.keys.add(entry.getKey());

            if (addReader)
                readersFlags.add(entry.getValue());
        }

        this.topVer = topVer;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.addDepInfo = addDepInfo;
        this.txLbl = txLbl;

        if (readThrough)
            flags |= READ_THROUGH_FLAG_MASK;

        if (skipVals)
            flags |= SKIP_VALS_FLAG_MASK;

        if (addReader)
            flags |= ADD_READER_FLAG_MASK;

        if (recovery)
            flags |= RECOVERY_FLAG_MASK;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * Gets task name hash.
     *
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Keys
     */
    public LinkedHashMap<KeyCacheObject, Boolean> keys() {
        return keyMap;
    }

    /**
     * @return Read through flag.
     */
    public boolean readThrough() {
        return (flags & READ_THROUGH_FLAG_MASK) != 0;
    }

    /**
     * @return Skip values flag. If true, boolean values indicating whether cache entry has a value will be
     *      returned as future result.
     */
    public boolean skipValues() {
        return (flags & SKIP_VALS_FLAG_MASK) != 0;
    }

    /**
     * @return Recovery flag.
     */
    public boolean recovery() {
        return (flags & RECOVERY_FLAG_MASK) != 0;
    }

    /** */
    public boolean addReaders() {
        return (flags & ADD_READER_FLAG_MASK) != 0;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return New TTL to set after entry is created, -1 to leave unchanged.
     */
    public long createTtl() {
        return createTtl;
    }

    /**
     * @return New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return keys != null && !keys.isEmpty() ? keys.get(0).partition() : -1;
    }

    /**
     * Get transaction label (may be null).
     *
     * @return Possible transaction label;
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert ctx != null;
        assert !F.isEmpty(keys);
        assert readersFlags == null || keys.size() == readersFlags.size();

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        assert !F.isEmpty(keys);
        assert readersFlags == null || keys.size() == readersFlags.size();

        if (keyMap == null) {
            keyMap = U.newLinkedHashMap(keys.size());

            Iterator<KeyCacheObject> keysIt = keys.iterator();

            for (int i = 0; i < keys.size(); i++) {
                Boolean addRdr = readersFlags != null ? readersFlags.get(i) : Boolean.FALSE;

                keyMap.put(keysIt.next(), addRdr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeLong(accessTtl))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong(createTtl))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte(flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeIgniteUuid(futId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeIgniteUuid(miniId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(readersFlags, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeInt(taskNameHash))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeAffinityTopologyVersion(topVer))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeString(txLbl))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage(ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                accessTtl = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                createTtl = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                futId = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                keys = reader.readCollection(MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                miniId = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                readersFlags = reader.readCollection(MessageCollectionItemType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                taskNameHash = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                topVer = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                txLbl = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ver = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 49;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetRequest.class, this);
    }
}
