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

package org.apache.ignite.internal.binary;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Time;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TYPE_ID_POS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
public class BinaryFieldExtractionSelfTest extends GridCommonAbstractTest {
    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller() throws Exception {
        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null));
        marsh.setBinaryContext(U.binaryContext(marsh, new IgniteConfiguration().setBinaryConfiguration(new BinaryConfiguration())));

        return marsh;
    }

    /**
     * Checking the exception and its text when changing the typeId of a
     * BinaryField.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTypeIdOfBinaryField() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue timeVal = new TimeValue(11111L);
        DecimalValue decimalVal = new DecimalValue(BigDecimal.ZERO);

        BinaryObjectImpl timeValBinObj = toBinary(timeVal, marsh);
        BinaryObjectImpl decimalValBinObj = toBinary(decimalVal, marsh);

        BinaryField timeBinField = timeValBinObj.type().field("time");

        Field typeIdField = U.findField(timeBinField.getClass(), "typeId");
        typeIdField.set(timeBinField, decimalValBinObj.typeId());

        String expMsg = exceptionMessageOfDifferentTypeIdBinaryField(
            decimalValBinObj.typeId(),
            decimalVal.getClass().getName(),
            timeValBinObj.typeId(),
            timeVal.getClass().getName(),
            U.field(timeBinField, "fieldId"),
            timeBinField.name(),
            null
        );

        assertThrows(log, () -> timeBinField.value(timeValBinObj), BinaryObjectException.class, expMsg);
    }

    /**
     * Checking the exception and its text when changing the typeId of a
     * BinaryField in case of not finding the expected BinaryType.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTypeIdOfBinaryFieldCaseNotFoundExpectedTypeId() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue timeVal = new TimeValue(11111L);

        BinaryObjectImpl timeValBinObj = toBinary(timeVal, marsh);

        BinaryField timeBinField = timeValBinObj.type().field("time");

        int newTypeId = timeValBinObj.typeId() + 1;

        Field typeIdField = U.findField(timeBinField.getClass(), "typeId");
        typeIdField.set(timeBinField, newTypeId);

        String expMsg = exceptionMessageOfDifferentTypeIdBinaryField(
            newTypeId,
            null,
            timeValBinObj.typeId(),
            timeVal.getClass().getName(),
            U.field(timeBinField, "fieldId"),
            timeBinField.name(),
            null
        );

        assertThrows(log, () -> timeBinField.value(timeValBinObj), BinaryObjectException.class, expMsg);
    }

    /**
     * Check that when changing typeId of BinaryObject, when trying to get the
     * field value BinaryObjectException will be thrown with the corresponding
     * text.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTypeIdOfBinaryFieldCaseNotFoundActualTypeId() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue timeVal = new TimeValue(11111L);

        BinaryObjectImpl timeValBinObj = toBinary(timeVal, marsh);

        BinaryField timeBinField = timeValBinObj.type().field("time");

        int beforeTypeId = timeValBinObj.typeId();

        String fieldType = binaryContext(marsh).metadata(timeValBinObj.typeId()).fieldTypeName(timeBinField.name());

        Field startField = U.findField(timeValBinObj.getClass(), "start");
        int start = (int)startField.get(timeValBinObj);

        Field arrField = U.findField(timeValBinObj.getClass(), "arr");
        byte[] arr = (byte[])arrField.get(timeValBinObj);
        arr[start + TYPE_ID_POS] += 1;

        String expMsg = exceptionMessageOfDifferentTypeIdBinaryField(
            beforeTypeId,
            timeVal.getClass().getName(),
            timeValBinObj.typeId(),
            null,
            U.field(timeBinField, "fieldId"),
            timeBinField.name(),
            fieldType
        );

        assertThrows(log, () -> timeBinField.value(timeValBinObj), BinaryObjectException.class, expMsg);
    }

    /**
     * @param obj Object to transform to a binary object.
     * @param marsh Binary marshaller.
     * @return Binary object.
     */
    protected BinaryObjectImpl toBinary(Object obj, BinaryMarshaller marsh) throws Exception {
        byte[] bytes = marsh.marshal(obj);

        return new BinaryObjectImpl(binaryContext(marsh), bytes, 0);
    }

    /**
     * Get binary context for the current marshaller.
     *
     * @param marsh Marshaller.
     * @return Binary context.
     */
    protected static BinaryContext binaryContext(BinaryMarshaller marsh) {
        GridBinaryMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /**
     *
     */
    private static class TestObject {
        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int iVal;

        /** */
        private long lVal;

        /** */
        private float fVal;

        /** */
        private double dVal;

        /**
         * @param seed Seed.
         */
        private TestObject(long seed) {
            bVal = (byte)seed;
            cVal = (char)seed;
            sVal = (short)seed;
            iVal = (int)seed;
            lVal = seed;
            fVal = seed;
            dVal = seed;
        }
    }

    /** */
    private static class TimeValue {
        /** */
        private Time time;

        /**
         * @param time Time.
         */
        TimeValue(long time) {
            this.time = new Time(time);
        }
    }

    /**
     *
     */
    private static class DecimalValue {
        /** */
        private BigDecimal decVal;

        /**
         *
         * @param decVal Value to use
         */
        private DecimalValue(BigDecimal decVal) {
            this.decVal = decVal;
        }
    }

    /**
     * Creates an exception text for the case when the typeId differs in the
     * BinaryField and the BinaryObject.
     *
     * @param expTypeId Expected typeId.
     * @param expTypeName Expected typeName.
     * @param actualTypeId Actual typeId.
     * @param actualTypeName Actual typeName.
     * @param fieldId FieldId.
     * @param fieldName FieldName.
     * @param fieldType FieldType.
     * @return Exception message.
     */
    private String exceptionMessageOfDifferentTypeIdBinaryField(
        int expTypeId,
        String expTypeName,
        int actualTypeId,
        String actualTypeName,
        int fieldId,
        String fieldName,
        String fieldType
    ) {
        return "Failed to get field because type ID of passed object differs from type ID this " +
            "BinaryField belongs to [expected=[typeId=" + expTypeId + ", typeName=" + expTypeName +
            "], actual=[typeId=" + actualTypeId + ", typeName=" + actualTypeName + "], fieldId=" + fieldId +
            ", fieldName=" + fieldName + ", fieldType=" + fieldType + "]";
    }
}
