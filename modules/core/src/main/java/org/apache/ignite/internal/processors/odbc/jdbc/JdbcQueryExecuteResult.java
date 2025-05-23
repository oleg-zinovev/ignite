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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResultMarshaler;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query execute result.
 */
public class JdbcQueryExecuteResult extends JdbcResult {
    /** Cursor ID. */
    private long cursorId;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /** Partition result. */
    private PartitionResult partRes;

    /** Transaction id. */
    private int txId;

    /**
     * Constructor.
     */
    JdbcQueryExecuteResult() {
        super(QRY_EXEC);
    }

    /**
     * @param cursorId Cursor ID.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     * @param partRes partition result to use for best affort affinity on the client side.
     * @param txId Transaction id.
     */
    JdbcQueryExecuteResult(long cursorId, List<List<Object>> items, boolean last, PartitionResult partRes, int txId) {
        super(QRY_EXEC);

        this.cursorId = cursorId;
        this.items = items;
        this.last = last;
        isQuery = true;
        this.partRes = partRes;
        this.txId = txId;
    }

    /**
     * @param cursorId Cursor ID.
     * @param updateCnt Update count for DML queries.
     * @param partRes partition result to use for best affort affinity on the client side.
     * @param txId Transaction id.
     */
    public JdbcQueryExecuteResult(long cursorId, long updateCnt, PartitionResult partRes, int txId) {
        super(QRY_EXEC);

        this.cursorId = cursorId;
        last = true;
        isQuery = false;
        this.updateCnt = updateCnt;
        this.partRes = partRes;
        this.txId = txId;
    }

    /**
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Update count for DML queries.
     */
    public long updateCount() {
        return updateCnt;
    }

    /**
     * @return Transaction id.
     */
    public int txId() {
        return txId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterEx writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeLong(cursorId);
        writer.writeBoolean(isQuery);

        if (isQuery) {
            assert items != null;

            writer.writeBoolean(last);

            JdbcUtils.writeItems(writer, items, protoCtx);
        }
        else
            writer.writeLong(updateCnt);

        writer.writeBoolean(partRes != null);

        if (protoCtx.isAffinityAwarenessSupported() && partRes != null)
            PartitionResultMarshaler.marshal(writer, partRes);

        if (protoCtx.isFeatureSupported(JdbcThinFeature.TX_AWARE_QUERIES))
            writer.writeInt(txId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderEx reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        cursorId = reader.readLong();
        isQuery = reader.readBoolean();

        if (isQuery) {
            last = reader.readBoolean();

            items = JdbcUtils.readItems(reader, protoCtx);
        }
        else {
            last = true;

            updateCnt = reader.readLong();
        }

        if (protoCtx.isAffinityAwarenessSupported() && reader.readBoolean())
            partRes = PartitionResultMarshaler.unmarshal(reader);

        if (protoCtx.isFeatureSupported(JdbcThinFeature.TX_AWARE_QUERIES))
            txId = reader.readInt();
    }

    /**
     * @return Partition result.
     */
    public PartitionResult partitionResult() {
        return partRes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteResult.class, this, super.toString());
    }
}
