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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReflectiveSerializer;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.internal.DuplicateTypeIdException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.MarshallerContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Binary context.
 */
public class BinaryContext {
    /** System loader. */
    private static final ClassLoader sysLdr = U.gridClassLoader();

    /** */
    private static final BinaryInternalMapper DFLT_MAPPER =
        new BinaryInternalMapper(new BinaryBasicNameMapper(false), new BinaryBasicIdMapper(true), false);

    /** */
    static final BinaryInternalMapper SIMPLE_NAME_LOWER_CASE_MAPPER =
        new BinaryInternalMapper(new BinaryBasicNameMapper(true), new BinaryBasicIdMapper(true), false);

    /** */
    private final ConcurrentMap<Class<?>, BinaryClassDescriptor> descByCls = new ConcurrentHashMap<>();

    /** */
    private final Map<Integer, BinaryClassDescriptor> predefinedTypes = new HashMap<>();

    /** */
    private final Map<String, Integer> predefinedTypeNames = new HashMap<>();

    /** */
    private final Map<Class<? extends Collection>, Byte> colTypes = new HashMap<>();

    /** */
    private final Map<Class<? extends Map>, Byte> mapTypes = new HashMap<>();

    /** Maps typeId to mappers. */
    private final ConcurrentMap<Integer, BinaryInternalMapper> typeId2Mapper = new ConcurrentHashMap<>(0);

    /** Affinity key field names. */
    private final ConcurrentMap<Integer, String> affKeyFieldNames = new ConcurrentHashMap<>(0);

    /** Maps className to mapper */
    private final ConcurrentMap<String, BinaryInternalMapper> cls2Mappers = new ConcurrentHashMap<>(0);

    /** Affinity key field names. */
    private final ConcurrentMap<Integer, BinaryIdentityResolver> identities = new ConcurrentHashMap<>(0);

    /** */
    private final BinaryMetadataHandler metaHnd;

    /** Node name. */
    private final @Nullable String igniteInstanceName;

    /** Class loader. */
    private final @Nullable ClassLoader clsLdr;

    /** Actual marshaller. */
    private BinaryMarshaller marsh;

    /** */
    private MarshallerContext marshCtx;

    /** */
    private final @Nullable BinarySerializer dfltSerializer;

    /** */
    private final Function<String, BinaryInternalMapper> mapperProvider;

    /** */
    private final Function<Class<?>, String> affFldNameProvider;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final OptimizedMarshaller optmMarsh = new OptimizedMarshaller(false);

    /** Compact footer flag. */
    private final boolean compactFooter;

    /** Object schemas. */
    private volatile Map<Integer, BinarySchemaRegistry> schemas;

    /**
     * @param metaHnd Meta data handler.
     * @param marsh Binary marshaller.
     * @param igniteInstanceName Ignite instance name.
     * @param clsLdr Class loader.
     * @param dfltSerializer Binary serializer.
     * @param idMapper Binary id mapper.
     * @param nameMapper Binary name mapper.
     * @param affFlds Affinity fields.
     * @param compactFooter Compact footer flag.
     * @param affFldNameProvider Affinity field name provider function.
     * @param log Logger.
     */
    public BinaryContext(
        BinaryMetadataHandler metaHnd,
        @Nullable BinaryMarshaller marsh,
        @Nullable String igniteInstanceName,
        @Nullable ClassLoader clsLdr,
        @Nullable BinarySerializer dfltSerializer,
        @Nullable BinaryIdMapper idMapper,
        @Nullable BinaryNameMapper nameMapper,
        @Nullable Collection<BinaryTypeConfiguration> typeCfgs,
        Map<String, String> affFlds,
        boolean compactFooter,
        Function<Class<?>, String> affFldNameProvider,
        IgniteLogger log
    ) {
        assert metaHnd != null;

        optmMarsh.nodeName(igniteInstanceName);

        this.metaHnd = metaHnd;
        this.igniteInstanceName = igniteInstanceName;
        this.clsLdr = clsLdr;
        this.dfltSerializer = dfltSerializer;

        if (idMapper != null || nameMapper != null || !F.isEmpty(typeCfgs))
            mapperProvider = clsName -> resolveMapper(clsName, idMapper, nameMapper, typeCfgs);
        else
            mapperProvider = clsName -> DFLT_MAPPER;

        this.compactFooter = compactFooter;
        this.affFldNameProvider = affFldNameProvider;

        this.log = log;

        colTypes.put(ArrayList.class, GridBinaryMarshaller.ARR_LIST);
        colTypes.put(LinkedList.class, GridBinaryMarshaller.LINKED_LIST);
        colTypes.put(BinaryUtils.SINGLETON_LIST_CLS, GridBinaryMarshaller.SINGLETON_LIST);
        colTypes.put(HashSet.class, GridBinaryMarshaller.HASH_SET);
        colTypes.put(LinkedHashSet.class, GridBinaryMarshaller.LINKED_HASH_SET);

        mapTypes.put(HashMap.class, GridBinaryMarshaller.HASH_MAP);
        mapTypes.put(LinkedHashMap.class, GridBinaryMarshaller.LINKED_HASH_MAP);

        // IDs range from [0..200] is used by Java SDK API and GridGain legacy API

        registerPredefinedType(Object.class, GridBinaryMarshaller.OBJECT);
        registerPredefinedType(Byte.class, GridBinaryMarshaller.BYTE);
        registerPredefinedType(Boolean.class, GridBinaryMarshaller.BOOLEAN);
        registerPredefinedType(Short.class, GridBinaryMarshaller.SHORT);
        registerPredefinedType(Character.class, GridBinaryMarshaller.CHAR);
        registerPredefinedType(Integer.class, GridBinaryMarshaller.INT);
        registerPredefinedType(Long.class, GridBinaryMarshaller.LONG);
        registerPredefinedType(Float.class, GridBinaryMarshaller.FLOAT);
        registerPredefinedType(Double.class, GridBinaryMarshaller.DOUBLE);
        registerPredefinedType(String.class, GridBinaryMarshaller.STRING);
        registerPredefinedType(BigDecimal.class, GridBinaryMarshaller.DECIMAL);
        registerPredefinedType(Date.class, GridBinaryMarshaller.DATE);
        registerPredefinedType(Timestamp.class, GridBinaryMarshaller.TIMESTAMP);
        registerPredefinedType(Time.class, GridBinaryMarshaller.TIME);
        registerPredefinedType(UUID.class, GridBinaryMarshaller.UUID);

        registerPredefinedType(byte[].class, GridBinaryMarshaller.BYTE_ARR);
        registerPredefinedType(short[].class, GridBinaryMarshaller.SHORT_ARR);
        registerPredefinedType(int[].class, GridBinaryMarshaller.INT_ARR);
        registerPredefinedType(long[].class, GridBinaryMarshaller.LONG_ARR);
        registerPredefinedType(float[].class, GridBinaryMarshaller.FLOAT_ARR);
        registerPredefinedType(double[].class, GridBinaryMarshaller.DOUBLE_ARR);
        registerPredefinedType(char[].class, GridBinaryMarshaller.CHAR_ARR);
        registerPredefinedType(boolean[].class, GridBinaryMarshaller.BOOLEAN_ARR);
        registerPredefinedType(BigDecimal[].class, GridBinaryMarshaller.DECIMAL_ARR);
        registerPredefinedType(String[].class, GridBinaryMarshaller.STRING_ARR);
        registerPredefinedType(UUID[].class, GridBinaryMarshaller.UUID_ARR);
        registerPredefinedType(Date[].class, GridBinaryMarshaller.DATE_ARR);
        registerPredefinedType(Timestamp[].class, GridBinaryMarshaller.TIMESTAMP_ARR);
        registerPredefinedType(Time[].class, GridBinaryMarshaller.TIME_ARR);
        registerPredefinedType(Object[].class, GridBinaryMarshaller.OBJ_ARR);

        // Special collections.
        registerPredefinedType(ArrayList.class, 0);
        registerPredefinedType(LinkedList.class, 0);
        registerPredefinedType(HashSet.class, 0);
        registerPredefinedType(LinkedHashSet.class, 0);
        registerPredefinedType(HashMap.class, 0);
        registerPredefinedType(LinkedHashMap.class, 0);

        registerPredefinedType(GridMapEntry.class, 60);
        registerPredefinedType(IgniteBiTuple.class, 61);
        registerPredefinedType(T2.class, 62);
        registerPredefinedType(IgniteUuid.class, 63);

        registerPredefinedType(BinaryObjectImpl.class, 0);
        registerPredefinedType(BinaryObjectOffheapImpl.class, 0);
        registerPredefinedType(BinaryMetadata.class, 0);
        registerPredefinedType(BinaryEnumObjectImpl.class, 0);
        registerPredefinedType(BinaryTreeMap.class, 0);
        registerPredefinedType(BinaryArray.class, 0);
        registerPredefinedType(BinaryEnumArray.class, 0);

        // BinaryUtils.FIELDS_SORTED_ORDER support, since it uses TreeMap at BinaryMetadata.
        registerBinarilizableSystemClass(BinaryTreeMap.class);
        registerBinarilizableSystemClass(TreeMap.class);
        registerBinarilizableSystemClass(TreeSet.class);

        // IDs range [200..1000] is used by Ignite internal APIs.

        if (U.sunReflectionFactory() == null) {
            U.warn(log, "ReflectionFactory not found, deserialization of binary objects for classes without " +
                "default constructor is not possible");
        }

        if (marsh != null) {
            this.marsh = marsh;

            marshCtx = marsh.getContext();

            assert marshCtx != null;

            optmMarsh.setContext(marshCtx);

            configure(nameMapper, idMapper, dfltSerializer, typeCfgs, affFlds);
        }
    }

    /**
     * @return Logger.
     */
    public IgniteLogger log() {
        return log;
    }

    /**
     * @return Marshaller.
     */
    public BinaryMarshaller marshaller() {
        return marsh;
    }

    /**
     * Check whether class must be deserialized anyway.
     *
     * @param cls Class.
     * @return {@code True} if must be deserialized.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean mustDeserialize(Class cls) {
        BinaryClassDescriptor desc = descByCls.get(cls);

        if (desc == null)
            return marshCtx.isSystemType(cls.getName()) || serializerForClass(cls) == null;
        else
            return desc.useOptimizedMarshaller();
    }

    /**
     * @return Ignite instance name.
     */
    public String igniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return Class loader.
     */
    public ClassLoader classLoader() {
        return clsLdr;
    }

    /**
     * @param globalNameMapper Name mapper.
     * @param globalIdMapper ID mapper.
     * @param globalSerializer Serializer.
     * @param typeCfgs Type configurations.
     * @param affFields Type to affinity fields mapping.
     * @throws BinaryObjectException In case of error.
     */
    private void configure(
        BinaryNameMapper globalNameMapper,
        BinaryIdMapper globalIdMapper,
        BinarySerializer globalSerializer,
        Collection<BinaryTypeConfiguration> typeCfgs,
        Map<String, String> affFields
    ) throws BinaryObjectException {
        TypeDescriptors descs = new TypeDescriptors();

        if (typeCfgs != null) {
            for (BinaryTypeConfiguration typeCfg : typeCfgs) {
                String clsName = typeCfg.getTypeName();

                if (clsName == null)
                    throw new BinaryObjectException("Class name is required for binary type configuration.");

                // Resolve mapper.
                BinaryIdMapper idMapper = U.firstNotNull(typeCfg.getIdMapper(), globalIdMapper);
                BinaryNameMapper nameMapper = U.firstNotNull(typeCfg.getNameMapper(), globalNameMapper);
                BinarySerializer serializer = U.firstNotNull(typeCfg.getSerializer(), globalSerializer);
                BinaryIdentityResolver identity = BinaryArrayIdentityResolver.instance();

                BinaryInternalMapper mapper = resolveMapper(nameMapper, idMapper);

                if (clsName.endsWith(".*")) {
                    String pkgName = clsName.substring(0, clsName.length() - 2);

                    for (String clsName0 : classesInPackage(pkgName)) {
                        String affField = affFields.remove(clsName0);

                        if (affField == null) {
                            Class<?> cls = U.classForName(clsName0, null);

                            if (cls != null)
                                affField = affFldNameProvider.apply(cls);
                        }

                        descs.add(clsName0, mapper, serializer, identity, affField,
                            typeCfg.isEnum(), typeCfg.getEnumValues(), true);
                    }
                }
                else {
                    String affField = affFields.remove(clsName);

                    if (affField == null) {
                        Class<?> cls = U.classForName(clsName, null);

                        if (cls != null)
                            affField = affFldNameProvider.apply(cls);
                    }

                    descs.add(clsName, mapper, serializer, identity, affField,
                        typeCfg.isEnum(), typeCfg.getEnumValues(), false);
                }
            }
        }

        for (TypeDescriptor desc : descs.descriptors())
            registerUserType(desc.clsName, desc.mapper, desc.serializer, desc.identity, desc.affKeyFieldName,
                desc.isEnum, desc.enumMap);

        BinaryInternalMapper globalMapper = resolveMapper(globalNameMapper, globalIdMapper);

        // Put affinity field names for unconfigured types.
        for (Map.Entry<String, String> entry : affFields.entrySet()) {
            String typeName = entry.getKey();

            int typeId = globalMapper.typeId(typeName);

            affKeyFieldNames.putIfAbsent(typeId, entry.getValue());
        }
    }

    /**
     * @param nameMapper Name mapper.
     * @param idMapper ID mapper.
     * @return Mapper.
     */
    private static BinaryInternalMapper resolveMapper(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) {
        if ((nameMapper == null || (DFLT_MAPPER.nameMapper().equals(nameMapper)))
            && (idMapper == null || DFLT_MAPPER.idMapper().equals(idMapper)))
            return DFLT_MAPPER;

        if (nameMapper != null && nameMapper instanceof BinaryBasicNameMapper
            && ((BinaryBasicNameMapper)nameMapper).isSimpleName()
            && idMapper != null && idMapper instanceof BinaryBasicIdMapper
            && ((BinaryBasicIdMapper)idMapper).isLowerCase())
            return SIMPLE_NAME_LOWER_CASE_MAPPER;

        if (nameMapper == null)
            nameMapper = DFLT_MAPPER.nameMapper();

        if (idMapper == null)
            idMapper = DFLT_MAPPER.idMapper();

        return new BinaryInternalMapper(nameMapper, idMapper, true);
    }

    /**
     * @return Internal mapper used as default.
     */
    public static BinaryInternalMapper defaultMapper() {
        return DFLT_MAPPER;
    }

    /**
     * @return ID mapper used as default.
     */
    public static BinaryIdMapper defaultIdMapper() {
        return DFLT_MAPPER.idMapper();
    }

    /**
     * @return Name mapper used as default.
     */
    public static BinaryNameMapper defaultNameMapper() {
        return DFLT_MAPPER.nameMapper();
    }

    /**
     * @param pkgName Package name.
     * @return Class names.
     */
    @SuppressWarnings("ConstantConditions")
    private static Iterable<String> classesInPackage(String pkgName) {
        assert pkgName != null;

        Collection<String> clsNames = new ArrayList<>();

        ClassLoader ldr = U.gridClassLoader();

        String pkgPath = pkgName.replaceAll("\\.", "/");

        URL[] urls = U.classLoaderUrls(ldr);

        for (URL url : urls) {
            String proto = url.getProtocol().toLowerCase();

            if ("file".equals(proto)) {
                try {
                    File cpElement = new File(url.toURI());

                    if (cpElement.isDirectory()) {
                        File pkgDir = new File(cpElement, pkgPath);

                        if (pkgDir.isDirectory()) {
                            for (File file : pkgDir.listFiles()) {
                                String fileName = file.getName();

                                if (file.isFile() && fileName.toLowerCase().endsWith(".class"))
                                    clsNames.add(pkgName + '.' + fileName.substring(0, fileName.length() - 6));
                            }
                        }
                    }
                    else if (cpElement.isFile()) {
                        try {
                            JarFile jar = new JarFile(cpElement);

                            Enumeration<JarEntry> entries = jar.entries();

                            while (entries.hasMoreElements()) {
                                String entry = entries.nextElement().getName();

                                if (entry.startsWith(pkgPath) && entry.endsWith(".class")) {
                                    String clsName = entry.substring(pkgPath.length() + 1, entry.length() - 6);

                                    if (!clsName.contains("/") && !clsName.contains("\\"))
                                        clsNames.add(pkgName + '.' + clsName);
                                }
                            }
                        }
                        catch (IOException ignored) {
                            // No-op.
                        }
                    }
                }
                catch (URISyntaxException ignored) {
                    // No-op.
                }
            }
        }

        return clsNames;
    }

    /**
     * Attempts registration of the provided class. If the type is already registered, then an existing descriptor is
     * returned.
     *
     * @param cls Class to register.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @return Class descriptor ID.
     * @throws BinaryObjectException In case of error.
     */
    public int registerType(
        Class<?> cls,
        boolean registerMeta,
        boolean failIfUnregistered
    ) throws BinaryObjectException {
        return registerClass(cls, registerMeta, failIfUnregistered).typeId();
    }

    /**
     * @param cls Class.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @return Class descriptor ID.
     * @throws BinaryObjectException In case of error.
     */
    public int registerTypeLocally(
        Class<?> cls,
        boolean registerMeta,
        boolean failIfUnregistered
    ) throws BinaryObjectException {
        return registerClass(cls, registerMeta, failIfUnregistered, true).typeId();
    }

    /**
     * Attempts registration of the provided class. If the type is already registered, then an existing descriptor is
     * returned.
     *
     * @param cls Class to register.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @return Class descriptor.
     * @throws BinaryObjectException In case of error.
     */
    @NotNull BinaryClassDescriptor registerClass(
        Class<?> cls,
        boolean registerMeta,
        boolean failIfUnregistered
    ) throws BinaryObjectException {
        return registerClass(cls, registerMeta, failIfUnregistered, false);
    }

    /**
     * @param cls Class.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @param onlyLocReg {@code true} if descriptor need to register only locally when registration is required at all.
     * @return Class descriptor.
     * @throws BinaryObjectException In case of error.
     */
    @NotNull BinaryClassDescriptor registerClass(
        Class<?> cls,
        boolean registerMeta,
        boolean failIfUnregistered,
        boolean onlyLocReg
    ) throws BinaryObjectException {
        assert cls != null;

        BinaryClassDescriptor desc = descriptorForClass(cls);

        if (!desc.registered()) {
            if (failIfUnregistered)
                throw new UnregisteredClassException(cls);
            else
                desc = registerDescriptor(desc, registerMeta, onlyLocReg);
        }

        return desc;
    }

    /**
     * Register system class that should be marshalled with BinaryMarshaller.
     * @param cls Class to register.
     */
    public void registerBinarilizableSystemClass(Class<?> cls) {
        String clsName = cls.getName();

        descByCls.put(cls, systemClassDescriptor(cls, clsName, new BinaryReflectiveSerializer()));
    }

    /**
     * Registers binary type locally.
     *
     * @param binaryType Binary type to register.
     * @param failIfUnregistered Whether to fail when not registered.
     * @param platformId Platform ID (see {@link org.apache.ignite.internal.MarshallerPlatformIds}).
     */
    public void registerClassLocally(BinaryType binaryType, boolean failIfUnregistered, byte platformId) {
        metaHnd.addMetaLocally(binaryType.typeId(), binaryType, failIfUnregistered);
        registerUserClassName(binaryType.typeId(), binaryType.typeName(), failIfUnregistered, true, platformId);
    }

    /**
     * @param cls Class.
     * @return A descriptor for the given class. If the class hasn't been registered yet, then a new descriptor will be
     * created, but its {@link BinaryClassDescriptor#registered()} will be {@code false}.
     */
    @NotNull BinaryClassDescriptor descriptorForClass(Class<?> cls) {
        assert cls != null;

        BinaryClassDescriptor desc = descByCls.get(cls);

        if (desc != null)
            return desc;
        else
            return createDescriptorForClass(cls);
    }

    /**
     * @param cls Class to create a descriptor for.
     * @return A descriptor for the given class. The descriptor needs to be registered in order to be used.
     */
    @NotNull private BinaryClassDescriptor createDescriptorForClass(Class<?> cls) {
        String clsName = cls.getName();

        if (marshCtx.isSystemType(clsName))
            return systemClassDescriptor(cls, clsName, null);
        else {
            BinaryInternalMapper mapper = userTypeMapper(clsName);

            final String typeName = mapper.typeName(clsName);

            final int typeId = mapper.typeId(clsName);

            BinarySerializer serializer = serializerForClass(cls);

            // Firstly check annotations, then check in cache key configurations.
            String affFieldName = affFldNameProvider.apply(cls);
            if (affFieldName == null)
                affFieldName = affKeyFieldNames.get(typeId);

            return new BinaryClassDescriptor(this,
                cls,
                true,
                typeId,
                typeName,
                affFieldName,
                mapper,
                serializer,
                true,
                false
            );
        }
    }

    /**
     * @param cls Class.
     * @param clsName Class name.
     * @param serializer Serializer.
     * @return Binary class descriptor
     */
    private BinaryClassDescriptor systemClassDescriptor(Class<?> cls, String clsName, @Nullable BinarySerializer serializer) {
        return new BinaryClassDescriptor(this,
            cls,
            false,
            clsName.hashCode(),
            clsName,
            null,
            SIMPLE_NAME_LOWER_CASE_MAPPER,
            serializer,
            false,
            false
        );
    }

    /**
     * @param userType User type or not.
     * @param typeId Type ID.
     * @param ldr Class loader.
     * @param registerMeta If {@code true}, then metadata will be registered along with the type descriptor.
     * @return Class descriptor.
     */
    BinaryClassDescriptor descriptorForTypeId(
        boolean userType,
        int typeId,
        ClassLoader ldr,
        boolean registerMeta
    ) {
        assert typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

        //TODO: As a workaround for IGNITE-1358 we always check the predefined map before without checking 'userType'
        BinaryClassDescriptor desc = predefinedTypes.get(typeId);

        if (desc != null)
            return desc;

        if (ldr == null)
            ldr = sysLdr;

        Class cls;

        try {
            if (GridBinaryMarshaller.USE_CACHE.get()) {
                cls = marshCtx.getClass(typeId, ldr);

                desc = descByCls.get(cls);
            }
            else {
                String clsName = marshCtx.getClassName(JAVA_ID, typeId);

                if (clsName == null)
                    throw new ClassNotFoundException("Unknown type ID: " + typeId);

                cls = U.forName(clsName, ldr, null);

                desc = descByCls.get(cls);

                if (desc == null)
                    return createNoneCacheClassDescriptor(cls);
            }

        }
        catch (ClassNotFoundException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(sysLdr) && (desc = descriptorForTypeId(true, typeId, sysLdr, registerMeta)) != null)
                return desc;

            throw new BinaryInvalidTypeException(e);
        }
        catch (IgniteCheckedException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(sysLdr) && (desc = descriptorForTypeId(true, typeId, sysLdr, registerMeta)) != null)
                return desc;

            throw new BinaryObjectException("Failed resolve class for ID: " + typeId, e);
        }

        if (desc == null) {
            desc = registerClass(cls, registerMeta, false);

            assert desc.typeId() == typeId : "Duplicate typeId [typeId=" + typeId + ", cls=" + cls
                + ", desc=" + desc + "]";
        }

        return desc;
    }

    /**
     * Creates descriptor without registration.
     *
     * @param cls Class.
     * @return Binary class descriptor.
     */
    @NotNull private BinaryClassDescriptor createNoneCacheClassDescriptor(Class cls) {
        String clsName = cls.getName();

        BinaryInternalMapper mapper = userTypeMapper(clsName);

        int typeId = mapper.typeId(clsName);

        String typeName = mapper.typeName(clsName);

        BinarySerializer serializer = serializerForClass(cls);

        String affFieldName = affFldNameProvider.apply(cls);

        return new BinaryClassDescriptor(this,
            cls,
            true,
            typeId,
            typeName,
            affFieldName,
            mapper,
            serializer,
            true,
            true,
            false
        );
    }

    /**
     * Attempts registration of the provided {@link BinaryClassDescriptor} in the cluster.
     *
     * @param desc Class descriptor to register.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @param onlyLocReg {@code true} if descriptor need to register only locally when registration is required at all.
     * @return Registered class descriptor.
     */
    @NotNull BinaryClassDescriptor registerDescriptor(
        BinaryClassDescriptor desc,
        boolean registerMeta,
        boolean onlyLocReg
    ) {
        if (desc.userType())
            return registerUserClassDescriptor(desc, registerMeta, onlyLocReg);
        else {
            BinaryClassDescriptor regDesc = desc.makeRegistered();

            if (GridBinaryMarshaller.USE_CACHE.get()) {
                BinaryClassDescriptor old = descByCls.putIfAbsent(desc.describedClass(), regDesc);

                if (old != null)
                    return old;
            }

            return regDesc;
        }
    }

    /**
     * Attempts registration of the provided {@link BinaryClassDescriptor} in the cluster. The provided descriptor should correspond
     * to a user class.
     *
     * @param desc Class descriptor to register.
     * @param registerMeta If {@code true}, then metadata will be registered along with the class descriptor.
     * @param onlyLocReg {@code true} if descriptor need to register only locally.
     * @return Class descriptor.
     */
    @NotNull private BinaryClassDescriptor registerUserClassDescriptor(
        BinaryClassDescriptor desc,
        boolean registerMeta,
        boolean onlyLocReg
    ) {
        assert desc.userType() : "The descriptor doesn't correspond to a user class.";

        Class<?> cls = desc.describedClass();

        int typeId = desc.typeId();

        boolean registered = registerUserClassName(typeId, cls.getName(), false, onlyLocReg, JAVA_ID);

        if (registered) {
            BinaryClassDescriptor regDesc = desc.makeRegistered();

            if (registerMeta) {
                if (onlyLocReg)
                    metaHnd.addMetaLocally(typeId, regDesc.metadata(false).wrap(this), false);
                else
                    metaHnd.addMeta(typeId, regDesc.metadata(true).wrap(this), false);
            }

            descByCls.put(cls, regDesc);

            typeId2Mapper.putIfAbsent(typeId, regDesc.mapper());

            return regDesc;
        }
        else
            return desc;
    }

    /**
     * Get serializer for class taking in count default one.
     *
     * @param cls Class.
     * @return Serializer for class or {@code null} if none exists.
     */
    @Nullable private BinarySerializer serializerForClass(Class cls) {
        BinarySerializer serializer = defaultSerializer();

        if (serializer == null && canUseReflectiveSerializer(cls))
            serializer = new BinaryReflectiveSerializer();

        return serializer;
    }

    /**
     * @return Default serializer.
     */
    private BinarySerializer defaultSerializer() {
        return dfltSerializer;
    }

    /**
     * @param cls Collection class.
     * @return Collection type ID.
     */
    public byte collectionType(Class<? extends Collection> cls) {
        assert cls != null;

        Byte type = colTypes.get(cls);

        if (type != null)
            return type;

        return Set.class.isAssignableFrom(cls) ? GridBinaryMarshaller.USER_SET : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param cls Map class.
     * @return Map type ID.
     */
    public byte mapType(Class<? extends Map> cls) {
        assert cls != null;

        Byte type = mapTypes.get(cls);

        return type != null ? type : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        Integer id = predefinedTypeNames.get(SIMPLE_NAME_LOWER_CASE_MAPPER.typeName(typeName));

        if (id != null)
            return id;

        if (marshCtx.isSystemType(typeName))
            return typeName.hashCode();

        BinaryInternalMapper mapper = userTypeMapper(typeName);

        return mapper.typeId(typeName);
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        BinaryInternalMapper mapper = userTypeMapper(typeId);

        return mapper.fieldId(typeId, fieldName);
    }

    /**
     * @param typeId Type ID.
     * @return Instance of ID mapper.
     */
    BinaryInternalMapper userTypeMapper(int typeId) {
        BinaryInternalMapper mapper = typeId2Mapper.get(typeId);

        return mapper != null ? mapper : SIMPLE_NAME_LOWER_CASE_MAPPER;
    }

    /**
     * @param clsName Type name.
     * @return Instance of ID mapper.
     */
    BinaryInternalMapper userTypeMapper(String clsName) {
        BinaryInternalMapper mapper = cls2Mappers.get(clsName);

        if (mapper != null)
            return mapper;

        mapper = mapperProvider.apply(clsName);

        BinaryInternalMapper prevMap = cls2Mappers.putIfAbsent(clsName, mapper);

        if (prevMap != null && !mapper.equals(prevMap))
            throw new IgniteException("Different mappers [clsName=" + clsName + ", newMapper=" + mapper
                + ", prevMap=" + prevMap + "]");

        prevMap = typeId2Mapper.putIfAbsent(mapper.typeId(clsName), mapper);

        if (prevMap != null && !mapper.equals(prevMap))
            throw new IgniteException("Different mappers [clsName=" + clsName + ", newMapper=" + mapper
                + ", prevMap=" + prevMap + "]");

        return mapper;
    }

    /**
     * @param clsName Type name.
     * @param globalIdMapper Default id mapper.
     * @param globalNameMapper Default name mapper.
     * @param typeCfgs Type configurations.
     * @return Mapper according to configuration.
     */
    private static BinaryInternalMapper resolveMapper(
        String clsName,
        BinaryIdMapper globalIdMapper,
        BinaryNameMapper globalNameMapper,
        Collection<BinaryTypeConfiguration> typeCfgs
    ) {
        assert clsName != null;

        if (typeCfgs != null) {
            for (BinaryTypeConfiguration typeCfg : typeCfgs) {
                String typeCfgName = typeCfg.getTypeName();

                // Pattern.
                if (typeCfgName != null && typeCfgName.endsWith(".*")) {
                    String pkgName = typeCfgName.substring(0, typeCfgName.length() - 2);

                    int dotIdx = clsName.lastIndexOf('.');

                    if (dotIdx > 0) {
                        String typePkgName = clsName.substring(0, dotIdx);

                        if (pkgName.equals(typePkgName)) {
                            // Resolve mapper.
                            BinaryIdMapper idMapper = globalIdMapper;

                            if (typeCfg.getIdMapper() != null)
                                idMapper = typeCfg.getIdMapper();

                            BinaryNameMapper nameMapper = globalNameMapper;

                            if (typeCfg.getNameMapper() != null)
                                nameMapper = typeCfg.getNameMapper();

                            return resolveMapper(nameMapper, idMapper);
                        }
                    }
                }
            }
        }

        return resolveMapper(globalNameMapper, globalIdMapper);
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    public String userTypeName(String clsName) {
        BinaryInternalMapper mapper = userTypeMapper(clsName);

        return mapper.typeName(clsName);
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     */
    public void registerPredefinedType(Class<?> cls, int id) {
        registerPredefinedType(cls, id, null, true);
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     * @param affFieldName Affinity field name.
     */
    public void registerPredefinedType(Class<?> cls, int id, String affFieldName, boolean registered) {
        String simpleClsName = SIMPLE_NAME_LOWER_CASE_MAPPER.typeName(cls.getName());

        if (id == 0)
            id = SIMPLE_NAME_LOWER_CASE_MAPPER.typeId(simpleClsName);

        BinaryClassDescriptor desc = new BinaryClassDescriptor(
            this,
            cls,
            false,
            id,
            simpleClsName,
            affFieldName,
            SIMPLE_NAME_LOWER_CASE_MAPPER,
            new BinaryReflectiveSerializer(),
            false,
            registered /* registered */
        );

        predefinedTypeNames.put(simpleClsName, id);
        predefinedTypes.put(id, desc);

        descByCls.put(cls, desc);

        if (affFieldName != null)
            affKeyFieldNames.putIfAbsent(id, affFieldName);
    }

    /**
     * @param clsName Class name.
     * @param mapper ID mapper.
     * @param serializer Serializer.
     * @param identity Type identity.
     * @param affKeyFieldName Affinity key field name.
     * @param isEnum If enum.
     * @param enumMap Enum name to ordinal mapping.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void registerUserType(String clsName,
        BinaryInternalMapper mapper,
        @Nullable BinarySerializer serializer,
        @Nullable BinaryIdentityResolver identity,
        @Nullable String affKeyFieldName,
        boolean isEnum,
        @Nullable Map<String, Integer> enumMap) throws BinaryObjectException {
        assert mapper != null;

        Class<?> cls = null;

        try {
            cls = U.resolveClassLoader(null, classLoader()).loadClass(clsName);
        }
        catch (ClassNotFoundException | NoClassDefFoundError ignored) {
            // No-op.
        }

        String typeName = mapper.typeName(clsName);

        int id = mapper.typeId(clsName);

        //Workaround for IGNITE-1358
        if (predefinedTypes.get(id) != null)
            throw duplicateTypeIdException(clsName, id);

        if (typeId2Mapper.put(id, mapper) != null)
            throw duplicateTypeIdException(clsName, id);

        if (identity != null) {
            if (identities.put(id, identity) != null)
                throw duplicateTypeIdException(clsName, id);
        }

        if (affKeyFieldName != null) {
            if (affKeyFieldNames.put(id, affKeyFieldName) != null)
                throw duplicateTypeIdException(clsName, id);
        }

        cls2Mappers.put(clsName, mapper);

        Map<String, BinaryFieldMetadata> fieldsMeta = null;

        if (cls != null) {
            if (serializer == null) {
                // At this point we must decide whether to rely on Java serialization mechanics or not.
                // If no serializer is provided, we examine the class and if it doesn't contain non-trivial
                // serialization logic we are safe to fallback to reflective binary serialization.
                if (canUseReflectiveSerializer(cls))
                    serializer = new BinaryReflectiveSerializer();
            }

            BinaryClassDescriptor desc = new BinaryClassDescriptor(
                this,
                cls,
                true,
                id,
                typeName,
                affKeyFieldName,
                mapper,
                serializer,
                true,
                true
            );

            fieldsMeta = desc.fieldsMeta();

            descByCls.put(cls, desc);

            // Registering in order to support the interoperability between Java, C++ and .Net.
            // https://issues.apache.org/jira/browse/IGNITE-3455
            predefinedTypes.put(id, desc);
        }

        metaHnd.addMeta(id,
            new BinaryMetadata(id, typeName, fieldsMeta, affKeyFieldName, null, isEnum, enumMap).wrap(this), false);
    }

    /**
     * Register user types schemas.
     */
    public void registerUserTypesSchema() {
        for (BinaryClassDescriptor desc : predefinedTypes.values()) {
            if (desc.userType())
                desc.registerStableSchema();
        }
    }

    /**
     * Register "type ID to class name" mapping on all nodes to allow for mapping requests resolution form client.
     * Other {@link BinaryContext}'s "register" methods and method
     * {@link BinaryContext#registerClass(Class, boolean, boolean)} already call this functionality
     * so use this method only when registering class names whose {@link Class} is unknown.
     *
     * @param typeId Type ID.
     * @param clsName Class Name.
     * @param failIfUnregistered If {@code true} then throw {@link UnregisteredBinaryTypeException} with {@link
     * org.apache.ignite.internal.processors.marshaller.MappingExchangeResult} future instead of synchronously awaiting
     * for its completion.
     * @param onlyLocReg Whether to register only on the current node.
     * @param platformId Platform ID (see {@link org.apache.ignite.internal.MarshallerPlatformIds}).
     * @return {@code True} if the mapping was registered successfully.
     */
    public boolean registerUserClassName(
            int typeId,
            String clsName,
            boolean failIfUnregistered,
            boolean onlyLocReg,
            byte platformId) {
        IgniteCheckedException e = null;

        boolean res = false;

        try {
            res = onlyLocReg
                ? marshCtx.registerClassNameLocally(platformId, typeId, clsName)
                : marshCtx.registerClassName(platformId, typeId, clsName, failIfUnregistered);
        }
        catch (DuplicateTypeIdException dupEx) {
            // Ignore if trying to register mapped type name of the already registered class name and vise versa
            BinaryInternalMapper mapper = userTypeMapper(typeId);

            String oldName = dupEx.getRegisteredClassName();

            if (!(mapper.typeName(oldName).equals(clsName) || mapper.typeName(clsName).equals(oldName)))
                e = dupEx;
        }
        catch (IgniteCheckedException igniteEx) {
            e = igniteEx;
        }

        if (e != null)
            throw new BinaryObjectException("Failed to register class.", e);

        return res;
    }

    /**
     * Throw exception on class duplication.
     *
     * @param clsName Class name.
     * @param id Type id.
     */
    private static BinaryObjectException duplicateTypeIdException(String clsName, int id) {
        return new BinaryObjectException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');
    }

    /**
     * Check whether reflective serializer can be used for class.
     *
     * @param cls Class.
     * @return {@code True} if reflective serializer can be used.
     */
    private static boolean canUseReflectiveSerializer(Class cls) {
        return BinaryUtils.isBinarylizable(cls) || !BinaryUtils.isCustomJavaSerialization(cls);
    }

    /**
     * Create binary field.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Binary field.
     */
    public BinaryField createField(int typeId, String fieldName) {
        BinarySchemaRegistry schemaReg = schemaRegistry(typeId);

        BinaryInternalMapper mapper = userTypeMapper(typeId);

        int fieldId = mapper.fieldId(typeId, fieldName);

        return new BinaryFieldImpl(this, typeId, schemaReg, fieldName, fieldId);
    }

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId) throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata(typeId) : null;
    }

    /**
     * @param typeId Type ID
     * @return Meta data.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata0(typeId) : null;
    }

    /**
     * @return All metadata known to this node.
     */
    public Collection<BinaryType> metadata() throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata() : Collections.emptyList();
    }

    /**
     * @param typeId Type ID.
     * @param schemaId Schema ID.
     * @return Meta data.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata(typeId, schemaId) : null;
    }

    /**
     * Get affinity key field name for type. First consult to predefined configuration, then delegate to metadata.
     *
     * @param typeId Type ID.
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName(int typeId) {
        String res = affKeyFieldNames.get(typeId);

        if (res == null) {
            BinaryMetadata meta = metaHnd.metadata0(typeId);

            if (meta != null)
                res = meta.affinityKeyFieldName();
        }

        return res;
    }

    /**
     * @param typeId Type ID.
     * @return Type identity.
     */
    public BinaryIdentityResolver identity(int typeId) {
        BinaryIdentityResolver rslvr = identities.get(typeId);

        return rslvr != null ? rslvr : BinaryArrayIdentityResolver.instance();
    }

    /**
     * @param typeId Type ID.
     * @param meta Meta data.
     * @param failIfUnregistered Fail if unregistered.
     * @throws BinaryObjectException In case of error.
     */
    public void updateMetadata(int typeId, BinaryMetadata meta, boolean failIfUnregistered) throws BinaryObjectException {
        metaHnd.addMeta(typeId, meta.wrap(this), failIfUnregistered);
    }

    /**
     * @return Whether field IDs should be skipped in footer or not.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /** */
    public void updateMetaIfNeeded(
        BinaryWriterEx writer,
        BinaryType meta,
        int typeId,
        String typeName,
        String affFieldName,
        Map<String, BinaryFieldMetadata> fieldsMeta
    ) {
        BinarySchemaRegistry schemaReg = schemaRegistry(typeId);

        // Update metadata if needed.
        int schemaId = writer.schemaId();

        if (schemaReg.schema(schemaId) == null) {
            if (typeName == null) {
                assert meta != null;

                typeName = meta.typeName();
            }

            BinarySchema curSchema = ((BinaryWriterExImpl)writer).currentSchema();

            if (affFieldName == null)
                affFieldName = affinityKeyFieldName(typeId);

            registerUserClassName(typeId, typeName, writer.failIfUnregistered(), false, JAVA_ID);

            updateMetadata(typeId, new BinaryMetadata(typeId, typeName, fieldsMeta, affFieldName,
                Collections.singleton(curSchema), false, null), writer.failIfUnregistered());

            schemaReg.addSchema(curSchema.schemaId(), curSchema);
        }
    }

    /**
     * Get schema registry for type ID.
     *
     * @param typeId Type ID.
     * @return Schema registry for type ID.
     */
    BinarySchemaRegistry schemaRegistry(int typeId) {
        Map<Integer, BinarySchemaRegistry> schemas0 = schemas;

        if (schemas0 == null) {
            synchronized (this) {
                schemas0 = schemas;

                if (schemas0 == null) {
                    schemas0 = new HashMap<>();

                    BinarySchemaRegistry reg = new BinarySchemaRegistry();

                    schemas0.put(typeId, reg);

                    schemas = schemas0;

                    return reg;
                }
            }
        }

        BinarySchemaRegistry reg = schemas0.get(typeId);

        if (reg == null) {
            synchronized (this) {
                reg = schemas.get(typeId);

                if (reg == null) {
                    reg = new BinarySchemaRegistry();

                    schemas0 = new HashMap<>(schemas);

                    schemas0.put(typeId, reg);

                    schemas = schemas0;
                }
            }
        }

        return reg;
    }

    /**
     * Unregister all binary schemas.
     */
    public void unregisterBinarySchemas() {
        schemas = null;
    }

    /**
     * Unregisters the user types descriptors.
     **/
    public void unregisterUserTypeDescriptors() {
        Iterator<Map.Entry<Class<?>, BinaryClassDescriptor>> it = descByCls.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Class<?>, BinaryClassDescriptor> e = it.next();

            if (e.getValue().userType())
                it.remove();
        }

        // Unregister Serializable and Externalizable type descriptors.
        optmMarsh.clearClassDescriptorsCache();
    }

    /**
     * Returns instance of {@link OptimizedMarshaller}.
     *
     * @return Optimized marshaller.
     */
    OptimizedMarshaller optimizedMarsh() {
        return optmMarsh;
    }

    /**
     * Undeployment callback invoked when class loader is being undeployed.
     *
     * Some marshallers may want to clean their internal state that uses the undeployed class loader somehow.
     *
     * @param ldr Class loader being undeployed.
     */
    public void onUndeploy(ClassLoader ldr) {
        for (Iterator<Map.Entry<Class<?>, BinaryClassDescriptor>> it = descByCls.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Class<?>, BinaryClassDescriptor> e = it.next();

            // Never undeploy system types.
            if (e.getValue().userType()) {
                if (ldr.equals(e.getKey().getClassLoader()))
                    it.remove();
            }
        }

        optmMarsh.onUndeploy(ldr);

        U.clearClassCache(ldr);
    }

    /**
     * @param typeId Type ID.
     */
    public synchronized void removeType(int typeId) {
        schemas.remove(typeId);
    }

    /** */
    Collection<BinaryClassDescriptor> predefinedTypes() {
        return Collections.unmodifiableCollection(predefinedTypes.values());
    }

    /** Creates instance of {@link BinaryArray}. */
    public BinaryObject createBinaryArray(Class<?> compCls, Object[] pArr) {
        boolean isBinaryArr = BinaryObject.class.isAssignableFrom(compCls);

        String compClsName = isBinaryArr ? Object.class.getName() : compCls.getName();

        // In case of interface or multidimensional array rely on class name.
        // Interfaces and array not registered as binary types.
        BinaryClassDescriptor desc = descriptorForClass(compCls);

        if (compCls.isEnum() || compCls == BinaryEnumObjectImpl.class) {
            return new BinaryEnumArray(
                this,
                desc.registered() ? desc.typeId() : GridBinaryMarshaller.UNREGISTERED_TYPE_ID,
                compClsName,
                pArr
            );
        }
        else {
            return new BinaryArray(
                this,
                desc.registered() ? desc.typeId() : GridBinaryMarshaller.UNREGISTERED_TYPE_ID,
                compClsName,
                pArr
            );
        }
    }

    /**
     * Type descriptors.
     */
    private static class TypeDescriptors {
        /** Descriptors map. */
        private final Map<String, TypeDescriptor> descs = new LinkedHashMap<>();

        /**
         * Add type descriptor.
         *
         * @param clsName Class name.
         * @param mapper Mapper.
         * @param serializer Serializer.
         * @param identity Key hashing mode.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum flag.
         * @param enumMap Enum constants mapping.
         * @param canOverride Whether this descriptor can be override.
         * @throws BinaryObjectException If failed.
         */
        private void add(String clsName,
            BinaryInternalMapper mapper,
            BinarySerializer serializer,
            BinaryIdentityResolver identity,
            String affKeyFieldName,
            boolean isEnum,
            Map<String, Integer> enumMap,
            boolean canOverride)
            throws BinaryObjectException {
            TypeDescriptor desc = new TypeDescriptor(clsName,
                mapper,
                serializer,
                identity,
                affKeyFieldName,
                isEnum,
                enumMap,
                canOverride);

            TypeDescriptor oldDesc = descs.get(clsName);

            if (oldDesc == null)
                descs.put(clsName, desc);
            else
                oldDesc.override(desc);
        }

        /**
         * Get all collected descriptors.
         *
         * @return Descriptors.
         */
        private Iterable<TypeDescriptor> descriptors() {
            return descs.values();
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDescriptor {
        /** Class name. */
        private final String clsName;

        /** Mapper. */
        private BinaryInternalMapper mapper;

        /** Serializer. */
        private BinarySerializer serializer;

        /** Type identity. */
        private BinaryIdentityResolver identity;

        /** Affinity key field name. */
        private String affKeyFieldName;

        /** Enum flag. */
        private boolean isEnum;

        /** Enum ordinal to name mapping. */
        private Map<String, Integer> enumMap;

        /** Whether this descriptor can be override. */
        private boolean canOverride;

        /**
         * Constructor.
         *
         * @param clsName Class name.
         * @param mapper ID mapper.
         * @param serializer Serializer.
         * @param identity Key hashing mode.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum type.
         * @param enumMap Mapping of enum names to ordinals.
         * @param canOverride Whether this descriptor can be override.
         */
        private TypeDescriptor(String clsName, BinaryInternalMapper mapper,
            BinarySerializer serializer, BinaryIdentityResolver identity, String affKeyFieldName, boolean isEnum,
            Map<String, Integer> enumMap, boolean canOverride) {
            this.clsName = clsName;
            this.mapper = mapper;
            this.serializer = serializer;
            this.identity = identity;
            this.affKeyFieldName = affKeyFieldName;
            this.isEnum = isEnum;
            this.enumMap = enumMap;
            this.canOverride = canOverride;
        }

        /**
         * Override binary class descriptor.
         *
         * @param other Other descriptor.
         * @throws BinaryObjectException If failed.
         */
        private void override(TypeDescriptor other) throws BinaryObjectException {
            assert clsName.equals(other.clsName);

            if (canOverride) {
                mapper = other.mapper;
                serializer = other.serializer;
                identity = other.identity;
                affKeyFieldName = other.affKeyFieldName;
                isEnum = other.isEnum;
                enumMap = other.enumMap;
                canOverride = other.canOverride;
            }
            else if (!other.canOverride)
                throw new BinaryObjectException("Duplicate explicit class definition in configuration: " + clsName);
        }
    }
}
