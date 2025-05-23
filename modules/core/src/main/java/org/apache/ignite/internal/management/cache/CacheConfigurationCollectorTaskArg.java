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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.regex.Pattern;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for {@link CacheConfigurationCollectorTask}.
 */
public class CacheConfigurationCollectorTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of cache names. */
    private Collection<String> cacheNames;

    /** Cache name regexp. */
    private String regex;

    /**
     * Default constructor.
     */
    public CacheConfigurationCollectorTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames Collection of cache names.
     */
    public CacheConfigurationCollectorTaskArg(Collection<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /**
     * @param regex Cache name regexp.
     */
    public CacheConfigurationCollectorTaskArg(String regex) {
        // Checks, that regex is correct.
        Pattern.compile(regex);

        this.regex = regex;
    }

    /**
     * @return Collection of cache deployment IDs.
     */
    public Collection<String> getCacheNames() {
        return cacheNames;
    }

    /**
     * @return Cache name regexp.
     */
    public String getRegex() {
        return regex;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cacheNames);
        U.writeString(out, regex);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheNames = U.readCollection(in);
        regex = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfigurationCollectorTaskArg.class, this);
    }
}
