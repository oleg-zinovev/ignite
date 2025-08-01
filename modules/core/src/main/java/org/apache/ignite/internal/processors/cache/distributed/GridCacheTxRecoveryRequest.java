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

package org.apache.ignite.internal.processors.cache.distributed;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message sent to check that transactions related to transaction were prepared on remote node.
 */
public class GridCacheTxRecoveryRequest extends GridDistributedBaseMessage {
    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Expected number of transactions on node. */
    private int txNum;

    /** System transaction flag. */
    private boolean sys;

    /** {@code True} if should check only tx on near node. */
    private boolean nearTxCheck;

    /**
     * Empty constructor.
     */
    public GridCacheTxRecoveryRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param txNum Expected number of transactions on remote node.
     * @param nearTxCheck {@code True} if should check only tx on near node.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheTxRecoveryRequest(IgniteInternalTx tx,
        int txNum,
        boolean nearTxCheck,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean addDepInfo
    ) {
        super(tx.xidVersion(), 0, addDepInfo);

        nearXidVer = tx.nearXidVersion();
        sys = tx.system();

        this.futId = futId;
        this.miniId = miniId;
        this.txNum = txNum;
        this.nearTxCheck = nearTxCheck;
    }

    /**
     * @return {@code True} if should check only tx on near node.
     */
    public boolean nearTxCheck() {
        return nearTxCheck;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Expected number of transactions on node.
     */
    public int transactions() {
        return txNum;
    }

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txRecoveryMessageLogger();
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
            case 8:
                if (!writer.writeIgniteUuid(futId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeIgniteUuid(miniId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean(nearTxCheck))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage(nearXidVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean(sys))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeInt(txNum))
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
            case 8:
                futId = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                miniId = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                nearTxCheck = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                nearXidVer = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                sys = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                txNum = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryRequest.class, this, "super", super.toString());
    }
}
