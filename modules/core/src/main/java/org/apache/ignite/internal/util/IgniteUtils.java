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

package org.apache.ignite.internal.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UTFDataFormatException;
import java.lang.annotation.Annotation;
import java.lang.management.CompilationMXBean;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.ProtectionDomain;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.jar.JarFile;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.compute.ComputeTaskCancelledCheckedException;
import org.apache.ignite.internal.compute.ComputeTaskTimeoutCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBean;
import org.apache.ignite.internal.processors.cache.CacheClassLoaderMarker;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgnitePeerToPeerClassLoadingException;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import static java.util.Objects.isNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_HOSTNAME_VERIFIER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_IGNORE_LOCAL_HOST_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_CLASS_LOADER_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_JVM_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_DISCO_ORDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_START_ON_CLIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_USER_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SUCCESS_FILE;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.events.EventType.EVTS_ALL;
import static org.apache.ignite.events.EventType.EVTS_ALL_MINUS_METRIC_UPDATE;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_DATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_REGIONS_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.util.GridUnsafe.putObjectVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.staticFieldBase;
import static org.apache.ignite.internal.util.GridUnsafe.staticFieldOffset;

/**
 * Collection of utility methods used throughout the system.
 */
@SuppressWarnings({"UnusedReturnValue"})
public abstract class IgniteUtils extends CommonUtils {
    /** Logger. */
    private static final Logger log = Logger.getLogger(IgniteUtils.class.getName());

    /** */
    public static final long KB = 1024L;

    /** */
    public static final long MB = 1024L * 1024;

    /** */
    public static final long GB = 1024L * 1024 * 1024;

    /** Minimum checkpointing page buffer size (may be adjusted by Ignite). */
    public static final Long DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE = GB / 4;

    /** Default maximum checkpointing page buffer size (when recovery data stored in WAL). */
    public static final Long DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE_WAL_RECOVERY = 2 * GB;

    /**
     * Default maximum checkpointing page buffer size (when recovery data stored on checkpoint).
     * In this mode checkpoint duration can be twice as long as for mode with storing recovery data to WAL.
     * Also, checkpoint buffer pages can't be released during write recovery data phase, so we need larger buffer size.
     */
    public static final Long DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE_CP_RECOVERY = 5 * GB;

    /** @see IgniteSystemProperties#IGNITE_MBEAN_APPEND_CLASS_LOADER_ID */
    public static final boolean DFLT_MBEAN_APPEND_CLASS_LOADER_ID = true;

    /** {@code True} if {@code unsafe} should be used for array copy. */
    private static final boolean UNSAFE_BYTE_ARR_CP = unsafeByteArrayCopyAvailable();

    /** All grid event names. */
    private static final Map<Integer, String> GRID_EVT_NAMES = new HashMap<>();

    /** All grid events. */
    private static final int[] GRID_EVTS;

    /** Empty integers array. */
    public static final int[] EMPTY_INTS = new int[0];

    /** Empty longs array. */
    public static final long[] EMPTY_LONGS = new long[0];

    /** Empty strings array. */
    public static final String[] EMPTY_STRS = new String[0];

    /** Empty fields array. */
    public static final Field[] EMPTY_FIELDS = new Field[0];

    /** */
    public static final UUID[] EMPTY_UUIDS = new UUID[0];

    /** Default user version. */
    public static final String DFLT_USER_VERSION = "0";

    /** Cache for {@link GridPeerDeployAware} fields to speed up reflection. */
    private static final ConcurrentMap<String, IgniteBiTuple<Class<?>, Collection<Field>>> p2pFields =
        new ConcurrentHashMap<>();

    /** Default working directory name. */
    private static final String DEFAULT_WORK_DIR = "work";

    /** Thread dump message. */
    public static final String THREAD_DUMP_MSG = "Thread dump at ";

    /** Alphanumeric with underscore regexp pattern. */
    private static final Pattern ALPHANUMERIC_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z_0-9]+$");

    /** Length of numbered file name. */
    public static final int NUMBER_FILE_NAME_LENGTH = 16;

    /** Ignite package. */
    public static final String IGNITE_PKG = "org.apache.ignite.";

    /** Project home directory. */
    private static volatile GridTuple<String> ggHome;

    /** OS string. */
    private static final String osStr;

    /** JDK string. */
    private static String jdkStr;

    /** Indicates whether current OS is some version of Windows. */
    private static boolean win;

    /** Indicates whether current OS is UNIX flavor. */
    private static boolean unix;

    /** Indicates whether current OS is Linux flavor. */
    private static boolean linux;

    /** Indicates whether current OS is Mac OS. */
    private static boolean mac;

    /** Name of the JDK. */
    private static String jdkName;

    /** Name of the JVM implementation. */
    private static String jvmImplName;

    /** Will be set to {@code true} if detected a 32-bit JVM. */
    private static final boolean jvm32Bit;

    /** JMX domain as 'xxx.apache.ignite'. */
    public static final String JMX_DOMAIN = IgniteUtils.class.getName().substring(0, IgniteUtils.class.getName().
        indexOf('.', IgniteUtils.class.getName().indexOf('.') + 1));

    /** Network packet header. */
    public static final byte[] IGNITE_HEADER = intToBytes(0x00004747);

    /** Default buffer size = 4K. */
    private static final int BUF_SIZE = 4096;

    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** Long date format pattern for log messages. */
    public static final DateTimeFormatter LONG_DATE_FMT =
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * Short date format pattern for log messages in "quiet" mode.
     * Only time is included since we don't expect "quiet" mode to be used
     * for longer runs.
     */
    public static final DateTimeFormatter SHORT_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Debug date format. */
    public static final DateTimeFormatter DEBUG_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss,SSS").withZone(ZoneId.systemDefault());

    /** Date format for thread dumps. */
    private static final DateTimeFormatter THREAD_DUMP_FMT =
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    /** Cached local host address to make sure that every time the same local host is returned. */
    private static InetAddress locHost;

    /** Supplier of network interfaces. Could be used for tests purposes, must not be changed in production code. */
    public static InterfaceSupplier INTERFACE_SUPPLIER = NetworkInterface::getNetworkInterfaces;

    /** */
    static volatile long curTimeMillis = System.currentTimeMillis();

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = new HashMap<>(16, .5f);

    /** Boxed class map. */
    private static final Map<Class<?>, Class<?>> boxedClsMap = new HashMap<>(16, .5f);

    /** Class loader used to load Ignite. */
    private static final ClassLoader gridClassLoader = IgniteUtils.class.getClassLoader();

    /** MAC OS invalid argument socket error message. */
    public static final String MAC_INVALID_ARG_MSG = "On MAC OS you may have too many file descriptors open " +
        "(simple restart usually solves the issue)";

    /** */
    public static final String DELIM = "--------------------------------------------------------------------------------";

    /** Ignite Logging Directory. */
    public static final String IGNITE_LOG_DIR = System.getenv(IgniteSystemProperties.IGNITE_LOG_DIR);

    /** Ignite Work Directory. */
    public static final String IGNITE_WORK_DIR = System.getenv(IgniteSystemProperties.IGNITE_WORK_DIR);

    /** Random is used to get random server node to authentication from client node. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Clock timer. */
    private static Thread timer;

    /** Grid counter. */
    static int gridCnt;

    /** Mutex. */
    static final Object mux = new Object();

    /** Exception converters. */
    private static final Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>>
        exceptionConverters;

    /** */
    private static volatile IgniteBiTuple<Collection<String>, Collection<String>> cachedLocalAddr;

    /** */
    private static volatile IgniteBiTuple<Collection<String>, Collection<String>> cachedLocalAddrAllHostNames;

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    /** Object.hashCode() */
    private static Method hashCodeMtd;

    /** Object.equals(...) */
    private static Method equalsMtd;

    /** Object.toString() */
    private static Method toStringMtd;

    /** Empty local Ignite name. */
    public static final String LOC_IGNITE_NAME_EMPTY = new String();

    /** Local Ignite name thread local. */
    private static final ThreadLocal<String> LOC_IGNITE_NAME = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return LOC_IGNITE_NAME_EMPTY;
        }
    };

    /** Ignite MBeans disabled flag. */
    public static boolean IGNITE_MBEANS_DISABLED =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_MBEANS_DISABLED);

    /** Ignite test features enabled flag. */
    public static boolean IGNITE_TEST_FEATURES_ENABLED =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_TEST_FEATURES_ENABLED);

    /** For tests. */
    @SuppressWarnings("PublicField")
    public static boolean useTestBinaryCtx;

    /** */
    private static final boolean assertionsEnabled;

    /** Empty URL array. */
    private static final URL[] EMPTY_URL_ARR = new URL[0];

    /** Builtin class loader class.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Class bltClsLdrCls = defaultClassLoaderClass();

    /** Url class loader field.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Field urlClsLdrField = urlClassLoaderField();

    /** Dev only logging disabled. */
    private static final boolean devOnlyLogDisabled =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DEV_ONLY_LOGGING_DISABLED);

    /** JDK9: jdk.internal.loader.URLClassPath. */
    private static Class clsURLClassPath;

    /** JDK9: URLClassPath#getURLs. */
    private static Method mthdURLClassPathGetUrls;

    /** Byte count prefixes. */
    private static final String BYTE_CNT_PREFIXES = " KMGTPE";

    /**
     * Success file name property. This file is used with auto-restarting functionality when Ignite
     * is started by supplied ignite.{bat|sh} scripts.
     */
    public static final String IGNITE_SUCCESS_FILE_PROPERTY = System.getProperty(IGNITE_SUCCESS_FILE);

    /**
     * JMX remote system property. Setting this property registered the Java VM platform's MBeans and published
     * the Remote Method Invocation (RMI) connector via a private interface to allow JMX client applications
     * to monitor a local Java platform, that is, a Java VM running on the same machine as the JMX client.
     */
    public static final String IGNITE_JMX_REMOTE_PROPERTY = System.getProperty("com.sun.management.jmxremote");

    /*
     * Initializes enterprise check.
     */
    static {
        boolean assertionsEnabled0 = true;

        try {
            assert false;

            assertionsEnabled0 = false;
        }
        catch (AssertionError ignored) {
            assertionsEnabled0 = true;
        }
        finally {
            assertionsEnabled = assertionsEnabled0;
        }

        String osName = System.getProperty("os.name");

        String osLow = osName.toLowerCase();

        // OS type detection.
        if (osLow.contains("win"))
            win = true;
        else if (osLow.contains("mac os"))
            mac = true;
        else {
            // UNIXs flavors tokens.
            for (CharSequence os : new String[] {"ix", "inux", "olaris", "un", "ux", "sco", "bsd", "att"})
                if (osLow.contains(os)) {
                    unix = true;

                    break;
                }

            if (osLow.contains("inux"))
                linux = true;
        }

        String osArch = System.getProperty("os.arch");

        String javaRtName = System.getProperty("java.runtime.name");
        String javaRtVer = System.getProperty("java.runtime.version");
        String jdkName = System.getProperty("java.specification.name");
        String osVer = System.getProperty("os.version");
        String jvmImplVer = System.getProperty("java.vm.version");
        String jvmImplVendor = System.getProperty("java.vm.vendor");
        String jvmImplName = System.getProperty("java.vm.name");

        // Best effort to detect a 32-bit JVM.
        String jvmArchDataModel = System.getProperty("sun.arch.data.model");

        String jdkStr = javaRtName + ' ' + javaRtVer + ' ' + jvmImplVendor + ' ' + jvmImplName + ' ' +
            jvmImplVer;

        osStr = osName + ' ' + osVer + ' ' + osArch;

        // Copy auto variables to static ones.
        IgniteUtils.jdkName = jdkName;
        IgniteUtils.jdkStr = jdkStr;
        IgniteUtils.jvmImplName = jvmImplName;

        jvm32Bit = "32".equals(jvmArchDataModel);

        primitiveMap.put("byte", byte.class);
        primitiveMap.put("short", short.class);
        primitiveMap.put("int", int.class);
        primitiveMap.put("long", long.class);
        primitiveMap.put("float", float.class);
        primitiveMap.put("double", double.class);
        primitiveMap.put("char", char.class);
        primitiveMap.put("boolean", boolean.class);
        primitiveMap.put("void", void.class);

        boxedClsMap.put(byte.class, Byte.class);
        boxedClsMap.put(short.class, Short.class);
        boxedClsMap.put(int.class, Integer.class);
        boxedClsMap.put(long.class, Long.class);
        boxedClsMap.put(float.class, Float.class);
        boxedClsMap.put(double.class, Double.class);
        boxedClsMap.put(char.class, Character.class);
        boxedClsMap.put(boolean.class, Boolean.class);
        boxedClsMap.put(void.class, Void.class);

        // Disable hostname SSL verification for development and testing with self-signed certificates.
        if (Boolean.parseBoolean(System.getProperty(IGNITE_DISABLE_HOSTNAME_VERIFIER))) {
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                @Override public boolean verify(String hostname, SSLSession sslSes) {
                    return true;
                }
            });
        }

        // Event names initialization.
        Class<?>[] evtHolderClasses = new Class[] {EventType.class, DiscoveryCustomEvent.class};

        for (Class<?> cls : evtHolderClasses) {
            for (Field field : cls.getFields()) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(int.class)) {
                    if (field.getName().startsWith("EVT_")) {
                        try {
                            int type = field.getInt(null);

                            String prev = GRID_EVT_NAMES.put(type, field.getName().substring("EVT_".length()));

                            // Check for duplicate event types.
                            assert prev == null : "Duplicate event [type=" + type + ", name1=" + prev +
                                ", name2=" + field.getName() + ']';
                        }
                        catch (IllegalAccessException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
            }
        }

        // Event array initialization.
        GRID_EVTS = toIntArray(GRID_EVT_NAMES.keySet());

        // Sort for fast event lookup.
        Arrays.sort(GRID_EVTS);

        // We need to re-initialize EVTS_ALL and EVTS_ALL_MINUS_METRIC_UPDATE
        // because they may have been initialized to null before GRID_EVTS were initialized.
        if (EVTS_ALL == null || EVTS_ALL_MINUS_METRIC_UPDATE == null) {
            try {
                Field f1 = EventType.class.getDeclaredField("EVTS_ALL");
                Field f2 = EventType.class.getDeclaredField("EVTS_ALL_MINUS_METRIC_UPDATE");

                assert f1 != null;
                assert f2 != null;

                // We use unsafe operations to update static fields on interface because
                // they are treated as static final and cannot be updated via standard reflection.
                putObjectVolatile(staticFieldBase(f1), staticFieldOffset(f1), gridEvents());
                putObjectVolatile(staticFieldBase(f2), staticFieldOffset(f2), gridEvents(EVT_NODE_METRICS_UPDATED));

                assert EVTS_ALL != null;
                assert EVTS_ALL.length == GRID_EVTS.length;

                assert EVTS_ALL_MINUS_METRIC_UPDATE != null;
                assert EVTS_ALL_MINUS_METRIC_UPDATE.length == GRID_EVTS.length - 1;

                // Validate correctness.
                for (int type : GRID_EVTS) {
                    assert containsIntArray(EVTS_ALL, type);

                    if (type != EVT_NODE_METRICS_UPDATED)
                        assert containsIntArray(EVTS_ALL_MINUS_METRIC_UPDATE, type);
                }

                assert !containsIntArray(EVTS_ALL_MINUS_METRIC_UPDATE, EVT_NODE_METRICS_UPDATED);
            }
            catch (NoSuchFieldException e) {
                throw new IgniteException(e);
            }
        }

        exceptionConverters = Collections.unmodifiableMap(exceptionConverters());

        // Set the http.strictPostRedirect property to prevent redirected POST from being mapped to a GET.
        System.setProperty("http.strictPostRedirect", "true");

        for (Method mtd : Object.class.getMethods()) {
            if ("hashCode".equals(mtd.getName()))
                hashCodeMtd = mtd;
            else if ("equals".equals(mtd.getName()))
                equalsMtd = mtd;
            else if ("toString".equals(mtd.getName()))
                toStringMtd = mtd;
        }

        try {
            clsURLClassPath = Class.forName("jdk.internal.loader.URLClassPath");
            mthdURLClassPathGetUrls = clsURLClassPath.getMethod("getURLs");
        }
        catch (ReflectiveOperationException e) {
            clsURLClassPath = null;
            mthdURLClassPathGetUrls = null;
        }
    }

    /**
     * Gets IgniteClosure for an IgniteCheckedException class.
     *
     * @param clazz Class.
     * @return The IgniteClosure mapped to this exception class, or null if none.
     */
    public static C1<IgniteCheckedException, IgniteException> getExceptionConverter(Class<? extends IgniteCheckedException> clazz) {
        return exceptionConverters.get(clazz);
    }

    /**
     * Gets map with converters to convert internal checked exceptions to public API unchecked exceptions.
     *
     * @return Exception converters.
     */
    private static Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>>
        exceptionConverters() {
        Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>> m = new HashMap<>();

        m.put(IgniteInterruptedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteInterruptedException(e.getMessage(), (InterruptedException)e.getCause());
            }
        });

        m.put(IgniteFutureCancelledCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureCancelledException(e.getMessage(), e);
            }
        });

        m.put(IgniteFutureTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureTimeoutException(e.getMessage(), e);
            }
        });

        m.put(ClusterGroupEmptyCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ClusterGroupEmptyException(e.getMessage(), e);
            }
        });

        m.put(ClusterTopologyCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                ClusterTopologyException topEx = new ClusterTopologyException(e.getMessage(), e);

                ClusterTopologyCheckedException checked = (ClusterTopologyCheckedException)e;

                if (checked.retryReadyFuture() != null)
                    topEx.retryReadyFuture(new IgniteFutureImpl<>(checked.retryReadyFuture()));

                return topEx;
            }
        });

        m.put(IgniteDeploymentCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteDeploymentException(e.getMessage(), e);
            }
        });

        m.put(ComputeTaskTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ComputeTaskTimeoutException(e.getMessage(), e);
            }
        });

        m.put(ComputeTaskCancelledCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ComputeTaskCancelledException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxRollbackCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionRollbackException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxHeuristicCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionHeuristicException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                if (e.getCause() instanceof TransactionDeadlockException)
                    return new TransactionTimeoutException(e.getMessage(), e.getCause());

                return new TransactionTimeoutException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxOptimisticCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionOptimisticException(e.getMessage(), e);
            }
        });

        m.put(IgniteClientDisconnectedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteClientDisconnectedException(
                    ((IgniteClientDisconnectedCheckedException)e).reconnectFuture(),
                    e.getMessage(),
                    e);
            }
        });

        return m;
    }

    /**
     * Gets all plugin providers.
     *
     * @return Plugins.
     */
    public static List<PluginProvider> allPluginProviders() {
        List<PluginProvider> providers = new ArrayList<>();

        Iterable<PluginProvider> it = loadService(PluginProvider.class);

        for (PluginProvider provider : it)
            providers.add(provider);

        return providers;
    }

    /**
     * Gets all plugin providers.
     *
     * @param cfg Configuration.
     * @param includeClsPath Include classpath plugins on empty config.
     * @return Plugins.
     */
    public static List<PluginProvider> allPluginProviders(IgniteConfiguration cfg, boolean includeClsPath) {
        return cfg.getPluginProviders() != null && cfg.getPluginProviders().length > 0 ?
            Arrays.asList(cfg.getPluginProviders()) :
            includeClsPath ?
                allPluginProviders() :
                Collections.emptyList();
    }

    /**
     * Converts exception, but unlike {@link #convertException(IgniteCheckedException)}
     * does not wrap passed in exception if none suitable converter found.
     *
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static Exception convertExceptionNoWrap(IgniteCheckedException e) {
        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (Exception)e.getCause();

        return e;
    }

    /**
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static IgniteException convertException(IgniteCheckedException e) {
        IgniteClientDisconnectedException e0 = e.getCause(IgniteClientDisconnectedException.class);

        if (e0 != null) {
            assert e0.reconnectFuture() != null : e0;

            throw e0;
        }

        IgniteClientDisconnectedCheckedException disconnectedErr =
            e.getCause(IgniteClientDisconnectedCheckedException.class);

        if (disconnectedErr != null) {
            assert disconnectedErr.reconnectFuture() != null : disconnectedErr;

            e = disconnectedErr;
        }

        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (IgniteException)e.getCause();

        return new IgniteException(e.getMessage(), e);
    }

    /**
     * @return System time approximated by 10 ms.
     */
    public static long currentTimeMillis() {
        return curTimeMillis;
    }

    /**
     * Gets name for given grid event type.
     *
     * @param type Event type.
     * @return Event name.
     */
    public static String gridEventName(int type) {
        String name = GRID_EVT_NAMES.get(type);

        return name != null ? name : Integer.toString(type);
    }

    /**
     * Gets all event types.
     *
     * @param excl Optional exclude events.
     * @return All events minus excluded ones.
     */
    public static int[] gridEvents(final int... excl) {
        if (F.isEmpty(excl))
            return GRID_EVTS;

        List<Integer> evts = toIntList(GRID_EVTS, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return !containsIntArray(excl, i);
            }
        });

        return toIntArray(evts);
    }

    /**
     * @param discoSpi Discovery SPI.
     * @return {@code True} if ordering is supported.
     */
    public static boolean discoOrdered(DiscoverySpi discoSpi) {
        DiscoverySpiOrderSupport ann = getAnnotation(discoSpi.getClass(), DiscoverySpiOrderSupport.class);

        return ann != null && ann.value();
    }

    /**
     * @return Checks if disco ordering should be enforced.
     */
    public static boolean relaxDiscoveryOrdered() {
        return "true".equalsIgnoreCase(System.getProperty(IGNITE_NO_DISCO_ORDER));
    }

    /**
     * This method should be used for adding quick debug statements in code
     * while debugging. Calls to this method should never be committed to master.
     *
     * @param msg Message to debug.
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void debug(Object msg) {
        X.error(debugPrefix() + msg);
    }

    /**
     * @param log Logger.
     * @param msg Message.
     */
    public static void dumpStack(@Nullable IgniteLogger log, String msg) {
        error(log, "Dumping stack.", new Exception(msg));
    }

    /**
     * @return Common prefix for debug messages.
     */
    private static String debugPrefix() {
        return '<' + DEBUG_DATE_FMT.format(Instant.now()) + "><DEBUG><" +
            Thread.currentThread().getName() + '>' + ' ';
    }

    /**
     * Gets heap size in GB rounded to specified precision.
     *
     * @param node Node.
     * @param precision Precision.
     * @return Heap size in GB.
     */
    public static double heapSize(ClusterNode node, int precision) {
        return heapSize(Collections.singleton(node), precision);
    }

    /**
     * Gets total heap size in GB rounded to specified precision.
     *
     * @param nodes Nodes.
     * @param precision Precision.
     * @return Total heap size in GB.
     */
    public static double heapSize(Iterable<ClusterNode> nodes, int precision) {
        // In bytes.
        double heap = 0.0;

        for (ClusterNode n : nodesPerJvm(nodes)) {
            ClusterMetrics m = n.metrics();

            heap += Math.max(m.getHeapMemoryInitialized(), m.getHeapMemoryMaximum());
        }

        return roundedHeapSize(heap, precision);
    }

    /**
     * Gets total offheap size in GB rounded to specified precision.
     *
     * @param nodes Nodes.
     * @param precision Precision.
     * @return Total offheap size in GB.
     */
    public static double offheapSize(Iterable<ClusterNode> nodes, int precision) {
        // In bytes.
        double totalOffheap = 0.0;

        for (ClusterNode n : nodesPerJvm(nodes)) {
            Long val = n.<Long>attribute(ATTR_DATA_REGIONS_OFFHEAP_SIZE);

            if (val != null)
                totalOffheap += val;
        }

        return roundedHeapSize(totalOffheap, precision);
    }

    /**
     * Returns one representative node for each JVM.
     *
     * @param nodes Nodes.
     * @return Collection which contains only one representative node for each JVM.
     */
    private static Iterable<ClusterNode> nodesPerJvm(Iterable<ClusterNode> nodes) {
        Map<String, ClusterNode> grpMap = new HashMap<>();

        // Group by mac addresses and pid.
        for (ClusterNode node : nodes) {
            String grpId = node.attribute(ATTR_MACS) + "|" + node.attribute(ATTR_JVM_PID);

            if (!grpMap.containsKey(grpId))
                grpMap.put(grpId, node);
        }

        return grpMap.values();
    }

    /**
     * Returns current JVM maxMemory in the same format as {@link #heapSize(ClusterNode, int)}.
     *
     * @param precision Precision.
     * @return Maximum memory size in GB.
     */
    public static double heapSize(int precision) {
        return roundedHeapSize(Runtime.getRuntime().maxMemory(), precision);
    }

    /**
     * Rounded heap size in gigabytes.
     *
     * @param heap Heap.
     * @param precision Precision.
     * @return Rounded heap size.
     */
    private static double roundedHeapSize(double heap, int precision) {
        double rounded = new BigDecimal(heap / (1024 * 1024 * 1024d)).round(new MathContext(precision)).doubleValue();

        return rounded < 0.1 ? 0.1 : rounded;
    }

    /**
     * Performs thread dump and prints all available info to the given log with WARN logging level.
     *
     * @param log Logger.
     */
    public static void dumpThreads(@Nullable IgniteLogger log) {
        dumpThreads(log, false);
    }

    /**
     * Performs thread dump and prints all available info to the given log
     * with WARN or ERROR logging level depending on {@code isErrorLevel} parameter.
     *
     * @param log Logger.
     * @param isErrorLevel {@code true} if thread dump must be printed with ERROR logging level,
     *      {@code false} if thread dump must be printed with WARN logging level.
     */
    public static void dumpThreads(@Nullable IgniteLogger log, boolean isErrorLevel) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        final Set<Long> deadlockedThreadsIds = getDeadlockedThreadIds(mxBean);

        if (deadlockedThreadsIds.isEmpty())
            logMessage(log, "No deadlocked threads detected.", isErrorLevel);
        else
            logMessage(log, "Deadlocked threads detected (see thread dump below) " +
                "[deadlockedThreadsCnt=" + deadlockedThreadsIds.size() + ']', isErrorLevel);

        ThreadInfo[] threadInfos =
            mxBean.dumpAllThreads(mxBean.isObjectMonitorUsageSupported(), mxBean.isSynchronizerUsageSupported());

        GridStringBuilder sb = new GridStringBuilder(THREAD_DUMP_MSG)
            .a(THREAD_DUMP_FMT.format(Instant.ofEpochMilli(currentTimeMillis()))).a(NL);

        for (ThreadInfo info : threadInfos) {
            printThreadInfo(info, sb, deadlockedThreadsIds);

            sb.a(NL);

            if (info.getLockedSynchronizers() != null && info.getLockedSynchronizers().length > 0) {
                printSynchronizersInfo(info.getLockedSynchronizers(), sb);

                sb.a(NL);
            }
        }

        sb.a(NL);

        logMessage(log, sb.toString(), isErrorLevel);
    }

    /**
     * @param log Logger.
     * @param msg Message.
     * @param isErrorLevel {@code true} if message must be printed with ERROR logging level,
     *      {@code false} if message must be printed with WARN logging level.
     */
    private static void logMessage(@Nullable IgniteLogger log, String msg, boolean isErrorLevel) {
        if (isErrorLevel)
            error(log, msg);
        else
            warn(log, msg);
    }

    /**
     * Get deadlocks from the thread bean.
     * @param mxBean the bean
     * @return the set of deadlocked threads (may be empty Set, but never null).
     */
    private static Set<Long> getDeadlockedThreadIds(ThreadMXBean mxBean) {
        final long[] deadlockedIds = mxBean.findDeadlockedThreads();

        final Set<Long> deadlockedThreadsIds;

        if (!F.isEmpty(deadlockedIds)) {
            Set<Long> set = new HashSet<>();

            for (long id : deadlockedIds)
                set.add(id);

            deadlockedThreadsIds = Collections.unmodifiableSet(set);
        }
        else
            deadlockedThreadsIds = Collections.emptySet();

        return deadlockedThreadsIds;
    }

    /**
     * @param threadId Thread ID.
     * @param sb Builder.
     */
    public static void printStackTrace(long threadId, GridStringBuilder sb) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        ThreadInfo threadInfo = mxBean.getThreadInfo(threadId, Integer.MAX_VALUE);

        printThreadInfo(threadInfo, sb, Collections.<Long>emptySet());
    }

    /**
     * @return {@code true} if there is java level deadlock.
     */
    public static boolean deadlockPresent() {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        return !F.isEmpty(mxBean.findDeadlockedThreads());
    }

    /**
     * Prints single thread info to a buffer.
     *
     * @param threadInfo Thread info.
     * @param sb Buffer.
     */
    private static void printThreadInfo(ThreadInfo threadInfo, GridStringBuilder sb, Set<Long> deadlockedIdSet) {
        final long id = threadInfo.getThreadId();

        if (deadlockedIdSet.contains(id))
            sb.a("##### DEADLOCKED ");

        sb.a("Thread [name=\"").a(threadInfo.getThreadName())
            .a("\", id=").a(threadInfo.getThreadId())
            .a(", state=").a(threadInfo.getThreadState())
            .a(", blockCnt=").a(threadInfo.getBlockedCount())
            .a(", waitCnt=").a(threadInfo.getWaitedCount()).a("]").a(NL);

        LockInfo lockInfo = threadInfo.getLockInfo();

        if (lockInfo != null) {
            sb.a("    Lock [object=").a(lockInfo)
                .a(", ownerName=").a(threadInfo.getLockOwnerName())
                .a(", ownerId=").a(threadInfo.getLockOwnerId()).a("]").a(NL);
        }

        MonitorInfo[] monitors = threadInfo.getLockedMonitors();
        StackTraceElement[] elements = threadInfo.getStackTrace();

        for (int i = 0; i < elements.length; i++) {
            StackTraceElement e = elements[i];

            sb.a("        at ").a(e.toString());

            for (MonitorInfo monitor : monitors) {
                if (monitor.getLockedStackDepth() == i)
                    sb.a(NL).a("        - locked ").a(monitor);
            }

            sb.a(NL);
        }
    }

    /**
     * Prints Synchronizers info to a buffer.
     *
     * @param syncs Synchronizers info.
     * @param sb Buffer.
     */
    private static void printSynchronizersInfo(LockInfo[] syncs, GridStringBuilder sb) {
        sb.a("    Locked synchronizers:");

        for (LockInfo info : syncs)
            sb.a(NL).a("        ").a(info);
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(@Nullable String cls, @Nullable Class<?> dflt) {
        return classForName(cls, dflt, false);
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @param includePrimitiveTypes Whether class resolution should include primitive types
     *                              (i.e. "int" will resolve to int.class if flag is set)
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(
        @Nullable String cls,
        @Nullable Class<?> dflt,
        boolean includePrimitiveTypes
    ) {
        Class<?> clazz;
        if (cls == null)
            clazz = dflt;
        else if (!includePrimitiveTypes || cls.length() > 7 || (clazz = primitiveMap.get(cls)) == null) {
            try {
                clazz = Class.forName(cls);
            }
            catch (ClassNotFoundException ignore) {
                clazz = dflt;
            }
        }
        return clazz;
    }

    /**
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class name.
     * @return Instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(String cls) throws IgniteCheckedException {
        Class<?> cls0;

        try {
            cls0 = Class.forName(cls);
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        return (T)newInstance(cls0);
    }

    /**
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(Class<T> cls) throws IgniteCheckedException {
        boolean set = false;

        Constructor<T> ctor = null;

        try {
            ctor = cls.getDeclaredConstructor();

            if (ctor == null)
                return null;

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return ctor.newInstance();
        }
        catch (NoSuchMethodException e) {
            throw new IgniteCheckedException("Failed to find empty constructor for class: " + cls, e);
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (ctor != null && set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Check whether class is in classpath.
     *
     * @return {@code True} if in classpath.
     */
    public static boolean inClassPath(String clsName) {
        try {
            Class.forName(clsName);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Pretty-formatting for minutes.
     *
     * @param mins Minutes to format.
     * @return Formatted presentation of minutes.
     */
    public static String formatMins(long mins) {
        assert mins >= 0;

        if (mins == 0)
            return "< 1 min";

        SB sb = new SB();

        long dd = mins / 1440; // 1440 mins = 60 mins * 24 hours

        if (dd > 0)
            sb.a(dd).a(dd == 1 ? " day " : " days ");

        mins %= 1440;

        long hh = mins / 60;

        if (hh > 0)
            sb.a(hh).a(hh == 1 ? " hour " : " hours ");

        mins %= 60;

        if (mins > 0)
            sb.a(mins).a(mins == 1 ? " min " : " mins ");

        return sb.toString().trim();
    }

    /**
     * Gets 8-character substring of UUID (for terse logging).
     *
     * @param id Input ID.
     * @return 8-character ID substring.
     */
    public static String id8(UUID id) {
        return id.toString().substring(0, 8);
    }

    /**
     * Gets 8-character substring of {@link org.apache.ignite.lang.IgniteUuid} (for terse logging).
     * The ID8 will be constructed as follows:
     * <ul>
     * <li>Take first 4 digits for global ID, i.e. {@link IgniteUuid#globalId()}.</li>
     * <li>Take last 4 digits for local ID, i.e. {@link IgniteUuid#localId()}.</li>
     * </ul>
     *
     * @param id Input ID.
     * @return 8-character representation of {@link IgniteUuid}.
     */
    public static String id8(IgniteUuid id) {
        String s = id.toString();

        return s.substring(0, 4) + s.substring(s.length() - 4);
    }

    /**
     * Writes array to output stream.
     *
     * @param out Output stream.
     * @param arr Array to write.
     * @param <T> Array type.
     * @throws IOException If failed.
     */
    public static <T> void writeArray(ObjectOutput out, T[] arr) throws IOException {
        int len = arr == null ? 0 : arr.length;

        out.writeInt(len);

        if (arr != null && arr.length > 0)
            for (T t : arr)
                out.writeObject(t);
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Object[] readArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Object[] arr = null;

        if (len > 0) {
            arr = new Object[len];

            for (int i = 0; i < len; i++)
                arr[i] = in.readObject();
        }

        return arr;
    }

    /**
     * Reads typed array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static <T> T[] readArray(ObjectInput in, Class<T> cls) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        T[] arr = null;

        if (len > 0) {
            arr = (T[])Array.newInstance(cls, len);

            for (int i = 0; i < len; i++)
                arr[i] = (T)in.readObject();
        }

        return arr;
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Class<?>[] readClassArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Class<?>[] arr = null;

        if (len > 0) {
            arr = new Class<?>[len];

            for (int i = 0; i < len; i++)
                arr[i] = (Class<?>)in.readObject();
        }

        return arr;
    }

    /**
     *
     * @param out Output.
     * @param col Set to write.
     * @throws IOException If write failed.
     */
    public static void writeCollection(ObjectOutput out, Collection<?> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Object o : col)
                out.writeObject(o);
        }
        else
            out.writeInt(-1);
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> Collection<E> readCollection(ObjectInput in)
        throws IOException, ClassNotFoundException {
        return readList(in);
    }

    /**
     *
     * @param m Map to seal.
     * @param <K> Key type.
     * @param <V> Value type
     * @return Sealed map.
     */
    public static <K, V> Map<K, V> sealMap(Map<K, V> m) {
        assert m != null;

        return Collections.unmodifiableMap(new HashMap<>(m));
    }

    /**
     * Seal collection.
     *
     * @param c Collection to seal.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(Collection<E> c) {
        return Collections.unmodifiableList(new ArrayList<>(c));
    }

    /**
     * Convert array to seal list.
     *
     * @param a Array for convert to seal list.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(E... a) {
        return Collections.unmodifiableList(Arrays.asList(a));
    }

    /**
     * Tries to resolve host by name, returning local host if input is empty.
     * This method reflects how {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} should
     * be handled in most places.
     *
     * @param hostName Hostname or {@code null} if local host should be returned.
     * @return Address of given host or of localhost.
     * @throws IOException If attempt to get local host failed.
     */
    public static InetAddress resolveLocalHost(@Nullable String hostName) throws IOException {
        return F.isEmpty(hostName) ?
            // Should default to InetAddress#anyLocalAddress which is package-private.
            new InetSocketAddress(0).getAddress() :
            InetAddress.getByName(hostName);
    }

    /**
     * @param addrs Addresses.
     * @return List of reachable addresses.
     */
    public static List<InetAddress> filterReachable(Collection<InetAddress> addrs) {
        if (addrs.isEmpty())
            return Collections.emptyList();

        final int reachTimeout = 2000;

        if (addrs.size() == 1) {
            InetAddress addr = F.first(addrs);

            if (reachable(addr, reachTimeout))
                return Collections.singletonList(addr);

            return Collections.emptyList();
        }

        final List<InetAddress> res = new ArrayList<>(addrs.size());

        Collection<Future<?>> futs = new ArrayList<>(addrs.size());

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(10, addrs.size()),
            new IgniteThreadFactory("utils", "reachable"));

        try {
            for (final InetAddress addr : addrs) {
                futs.add(executor.submit(new Runnable() {
                    @Override public void run() {
                        if (reachable(addr, reachTimeout)) {
                            synchronized (res) {
                                res.add(addr);
                            }
                        }
                    }
                }));
            }

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException("Thread has been interrupted.", e);
                }
                catch (ExecutionException e) {
                    throw new IgniteException(e);
                }
            }
        }
        finally {
            executor.shutdown();
        }

        return res;
    }

    /**
     * Returns host names consistent with {@link #resolveLocalHost(String)}. So when it returns
     * a common address this method returns single host name, and when a wildcard address passed
     * this method tries to collect addresses of all available interfaces.
     *
     * @param locAddr Local address to resolve.
     * @return Resolved available addresses of given local address.
     * @throws IOException If failed.
     */
    public static IgniteBiTuple<Collection<String>, Collection<String>> resolveLocalAddresses(InetAddress locAddr)
        throws IOException {
        return resolveLocalAddresses(locAddr, false);
    }

    /**
     * Returns host names consistent with {@link #resolveLocalHost(String)}. So when it returns
     * a common address this method returns single host name, and when a wildcard address passed
     * this method tries to collect addresses of all available interfaces.
     *
     * @param locAddr Local address to resolve.
     * @param allHostNames If {@code true} then include host names for all addresses.
     * @return Resolved available addresses and host names of given local address.
     * @throws IOException If failed.
     */
    public static IgniteBiTuple<Collection<String>, Collection<String>> resolveLocalAddresses(InetAddress locAddr,
        boolean allHostNames) throws IOException {
        assert locAddr != null;

        Collection<String> addrs = new ArrayList<>();
        Collection<String> hostNames = new ArrayList<>();

        if (locAddr.isAnyLocalAddress()) {
            IgniteBiTuple<Collection<String>, Collection<String>> res =
                allHostNames ? cachedLocalAddrAllHostNames : cachedLocalAddr;

            if (res == null) {
                List<InetAddress> locAddrs = new ArrayList<>();

                for (NetworkInterface itf : asIterable(INTERFACE_SUPPLIER.getInterfaces())) {
                    for (InetAddress addr : asIterable(itf.getInetAddresses())) {
                        if (!addr.isLinkLocalAddress())
                            locAddrs.add(addr);
                    }
                }

                locAddrs = filterReachable(locAddrs);

                for (InetAddress addr : locAddrs)
                    addresses(addr, addrs, hostNames, allHostNames);

                if (F.isEmpty(addrs))
                    return F.t(Collections.emptyList(), Collections.emptyList());

                res = F.t(addrs, hostNames);

                if (allHostNames)
                    cachedLocalAddrAllHostNames = res;
                else
                    cachedLocalAddr = res;
            }

            return res;
        }

        addresses(locAddr, addrs, hostNames, allHostNames);

        return F.t(addrs, hostNames);
    }

    /**
     * @param addr Address.
     * @param addrs Addresses.
     * @param allHostNames If {@code true} then include host names for all addresses.
     * @param hostNames Host names.
     */
    private static void addresses(InetAddress addr, Collection<String> addrs, Collection<String> hostNames,
        boolean allHostNames) {
        String ipAddr = addr.getHostAddress();

        addrs.add(ipAddr);

        boolean ignoreLocHostName = getBoolean(IGNITE_IGNORE_LOCAL_HOST_NAME, true);

        String userDefinedLocHost = getString(IGNITE_LOCAL_HOST);

        // If IGNITE_LOCAL_HOST is defined and IGNITE_IGNORE_LOCAL_HOST_NAME is not false, then ignore local address's hostname
        if (!F.isEmpty(userDefinedLocHost) && ignoreLocHostName)
            return;

        String hostName = addr.getHostName();

        if (allHostNames)
            hostNames.add(hostName);
        else if (!F.isEmpty(hostName) && !addr.isLoopbackAddress())
            hostNames.add(hostName);
    }

    /**
     * Gets local host. Implementation will first attempt to get a non-loopback
     * address. If that fails, then loopback address will be returned.
     * <p>
     * Note that this method is synchronized to make sure that local host
     * initialization happens only once.
     *
     * @return Address representing local host.
     * @throws IOException If attempt to get local host failed.
     */
    public static synchronized InetAddress getLocalHost() throws IOException {
        if (locHost == null)
            // Cache it.
            resetLocalHost();

        return locHost;
    }

    /**
     * @return Local host.
     * @throws IOException If attempt to get local host failed.
     */
    private static synchronized InetAddress resetLocalHost() throws IOException {
        locHost = null;

        String sysLocHost = IgniteSystemProperties.getString(IGNITE_LOCAL_HOST);

        if (sysLocHost != null)
            sysLocHost = sysLocHost.trim();

        if (!F.isEmpty(sysLocHost))
            locHost = InetAddress.getByName(sysLocHost);
        else {
            List<NetworkInterface> itfs = new ArrayList<>();

            for (NetworkInterface itf : asIterable(INTERFACE_SUPPLIER.getInterfaces()))
                itfs.add(itf);

            Collections.sort(itfs, new Comparator<NetworkInterface>() {
                @Override public int compare(NetworkInterface itf1, NetworkInterface itf2) {
                    // Interfaces whose name starts with 'e' should go first.
                    return itf1.getName().compareTo(itf2.getName());
                }
            });

            // It should not take longer than 2 seconds to reach
            // local address on any network.
            int reachTimeout = 2000;

            for (NetworkInterface itf : itfs) {
                boolean found = false;

                for (InetAddress addr : asIterable(itf.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && !addr.isLinkLocalAddress() && reachable(itf, addr, reachTimeout)) {
                        locHost = addr;

                        found = true;

                        break;
                    }
                }

                if (found)
                    break;
            }
        }

        if (locHost == null)
            locHost = InetAddress.getLocalHost();

        return locHost;
    }

    /**
     * Checks if address can be reached using three argument InetAddress.isReachable() version.
     *
     * @param itf Network interface to use for test.
     * @param addr Address to check.
     * @param reachTimeout Timeout for the check.
     * @return {@code True} if address is reachable.
     */
    public static boolean reachable(NetworkInterface itf, InetAddress addr, int reachTimeout) {
        try {
            return addr.isReachable(itf, 0, reachTimeout);
        }
        catch (IOException ignore) {
            return false;
        }
    }

    /**
     * Checks if address can be reached using one argument InetAddress.isReachable() version.
     *
     * @param addr Address to check.
     * @param reachTimeout Timeout for the check.
     * @return {@code True} if address is reachable.
     */
    public static boolean reachable(InetAddress addr, int reachTimeout) {
        try {
            return addr.isReachable(reachTimeout);
        }
        catch (IOException ignore) {
            return false;
        }
    }

    /**
     * @param loc Local node.
     * @param rmt Remote node.
     * @return Whether given nodes have the same macs.
     */
    public static boolean sameMacs(ClusterNode loc, ClusterNode rmt) {
        assert loc != null;
        assert rmt != null;

        String locMacs = loc.attribute(IgniteNodeAttributes.ATTR_MACS);
        String rmtMacs = rmt.attribute(IgniteNodeAttributes.ATTR_MACS);

        return locMacs != null && locMacs.equals(rmtMacs);
    }

    /**
     * Gets a list of all local non-loopback IPs known to this JVM.
     * Note that this will include both IPv4 and IPv6 addresses (even if one "resolves"
     * into another). Loopbacks will be skipped.
     *
     * @return List of all known local IPs (empty list if no addresses available).
     */
    public static synchronized Collection<String> allLocalIps() {
        List<String> ips = new ArrayList<>(4);

        try {
            Enumeration<NetworkInterface> itfs = INTERFACE_SUPPLIER.getInterfaces();

            if (itfs != null) {
                for (NetworkInterface itf : asIterable(itfs)) {
                    if (!itf.isLoopback()) {
                        Enumeration<InetAddress> addrs = itf.getInetAddresses();

                        for (InetAddress addr : asIterable(addrs)) {
                            String hostAddr = addr.getHostAddress();

                            if (!addr.isLoopbackAddress() && !ips.contains(hostAddr))
                                ips.add(hostAddr);
                        }
                    }
                }
            }
        }
        catch (SocketException ignore) {
            return Collections.emptyList();
        }

        Collections.sort(ips);

        return ips;
    }

    /**
     * Checks if the address is local.
     *
     * @param addr Address for check.
     * @return true if address is local, otherwise false
     */
    public static boolean isLocalAddress(InetAddress addr) {
        // Check if the address is a valid special local or loop back
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
            return true;

        // Check if the address is defined on any interface
        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        }
        catch (SocketException e) {
            return false;
        }
    }

    /**
     * Gets a list of all local enabled MACs known to this JVM. It
     * is using hardware address of the network interface that is not guaranteed to be
     * MAC addresses (but in most cases it is).
     * <p>
     * Note that if network interface is disabled - its MAC won't be included. All
     * local network interfaces are probed including loopbacks. Virtual interfaces
     * (sub-interfaces) are skipped.
     * <p>
     * Note that on linux getHardwareAddress() can return null from time to time
     * if NetworkInterface.getHardwareAddress() method is called from many threads.
     *
     * @return List of all known enabled local MACs or empty list
     *      if no MACs could be found.
     */
    public static synchronized Collection<String> allLocalMACs() {
        List<String> macs = new ArrayList<>(3);

        try {
            Enumeration<NetworkInterface> itfs = INTERFACE_SUPPLIER.getInterfaces();

            if (itfs != null) {
                for (NetworkInterface itf : asIterable(itfs)) {
                    byte[] hwAddr = itf.getHardwareAddress();

                    // Loopback produces empty MAC.
                    if (hwAddr != null && hwAddr.length > 0) {
                        String mac = byteArray2HexString(hwAddr);

                        if (!macs.contains(mac))
                            macs.add(mac);
                    }
                }
            }
        }
        catch (SocketException ignore) {
            return Collections.emptyList();
        }

        Collections.sort(macs);

        return macs;
    }

    /**
     * Replace password in URI string with a single '*' character.
     * <p>
     * Parses given URI by applying &quot;.*://(.*:.*)@.*&quot;
     * regular expression pattern and than if URI matches it
     * replaces password strings between '/' and '@' with '*'.
     *
     * @param uri URI which password should be replaced.
     * @return Converted URI string
     */
    @Nullable public static String hidePassword(@Nullable String uri) {
        if (uri == null)
            return null;

        if (Pattern.matches(".*://(.*:.*)@.*", uri)) {
            int userInfoLastIdx = uri.indexOf('@');

            assert userInfoLastIdx != -1;

            String str = uri.substring(0, userInfoLastIdx);

            int userInfoStartIdx = str.lastIndexOf('/');

            str = str.substring(userInfoStartIdx + 1);

            String[] params = str.split(";");

            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < params.length; i++) {
                int idx;

                if ((idx = params[i].indexOf(':')) != -1)
                    params[i] = params[i].substring(0, idx + 1) + '*';

                builder.append(params[i]);

                if (i != params.length - 1)
                    builder.append(';');
            }

            return new StringBuilder(uri).replace(userInfoStartIdx + 1, userInfoLastIdx,
                builder.toString()).toString();
        }

        return uri;
    }

    /**
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader gridClassLoader() {
        return gridClassLoader;
    }

    /**
     * @return ClassLoader at IgniteConfiguration in case it is not null or
     * ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(IgniteConfiguration cfg) {
        return resolveClassLoader(null, cfg);
    }

    /**
     * @return ClassLoader passed as param in case it is not null or
     * ClassLoader at IgniteConfiguration in case it is not null or
     * ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(@Nullable ClassLoader ldr, IgniteConfiguration cfg) {
        assert cfg != null;

        return resolveClassLoader(ldr, cfg.getClassLoader());
    }

    /**
     * @param ldr Custom class loader.
     * @param cfgLdr Class loader from config.
     * @return ClassLoader passed as param in case it is not null or cfgLdr  in case it is not null or ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(@Nullable ClassLoader ldr, @Nullable ClassLoader cfgLdr) {
        return (ldr != null && ldr != gridClassLoader)
            ? ldr
            : cfgLdr != null
                ? cfgLdr
                : gridClassLoader;
    }

    /**
     * @param parent Parent to find.
     * @param ldr Loader to check.
     * @return {@code True} if parent found.
     */
    public static boolean hasParent(@Nullable ClassLoader parent, ClassLoader ldr) {
        if (parent != null) {
            for (; ldr != null; ldr = ldr.getParent()) {
                if (ldr.equals(parent))
                    return true;
            }

            return false;
        }

        return true;
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            out.write(arr);
        }
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr, int maxLen) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            int len = Math.min(arr.length, maxLen);

            out.writeInt(len);

            out.write(arr, 0, len);
        }
    }

    /**
     * Reads byte array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws java.io.IOException If read failed.
     */
    @Nullable public static byte[] readByteArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        byte[] res = new byte[len];

        in.readFully(res);

        return res;
    }

    /**
     * Join byte arrays into single one.
     *
     * @param bufs list of byte arrays to concatenate.
     * @return Concatenated byte's array.
     */
    public static byte[] join(byte[]... bufs) {
        int size = 0;
        for (byte[] buf : bufs) {
            size += buf.length;
        }

        byte[] res = new byte[size];
        int position = 0;
        for (byte[] buf : bufs) {
            arrayCopy(buf, 0, res, position, buf.length);
            position += buf.length;
        }

        return res;
    }

    /**
     * Convert string with hex values to byte array.
     *
     * @param hex Hexadecimal string to convert.
     * @return array of bytes defined as hex in string.
     * @throws IllegalArgumentException If input character differs from certain hex characters.
     */
    public static byte[] hexString2ByteArray(String hex) throws IllegalArgumentException {
        // If Hex string has odd character length.
        if (hex.length() % 2 != 0)
            hex = '0' + hex;

        char[] chars = hex.toCharArray();

        byte[] bytes = new byte[chars.length / 2];

        int byteCnt = 0;

        for (int i = 0; i < chars.length; i += 2) {
            int newByte = 0;

            newByte |= hexCharToByte(chars[i]);

            newByte <<= 4;

            newByte |= hexCharToByte(chars[i + 1]);

            bytes[byteCnt] = (byte)newByte;

            byteCnt++;
        }

        return bytes;
    }

    /**
     * Gets a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexLong(long val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Gets a hex string representation of the given int value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexInt(int val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Return byte value for certain character.
     *
     * @param ch Character
     * @return Byte value.
     * @throws IllegalArgumentException If input character differ from certain hex characters.
     */
    private static byte hexCharToByte(char ch) throws IllegalArgumentException {
        switch (ch) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return (byte)(ch - '0');

            case 'a':
            case 'A':
                return 0xa;

            case 'b':
            case 'B':
                return 0xb;

            case 'c':
            case 'C':
                return 0xc;

            case 'd':
            case 'D':
                return 0xd;

            case 'e':
            case 'E':
                return 0xe;

            case 'f':
            case 'F':
                return 0xf;

            default:
                throw new IllegalArgumentException("Hex decoding wrong input character [character=" + ch + ']');
        }
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return toBytes(l, new byte[8], 0, 8);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        toBytes(l, bytes, off, 8);

        return off + 8;
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return toBytes(i, new byte[4], 0, 4);
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        toBytes(i, bytes, off, 4);

        return off + 4;
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return toBytes(s, new byte[2], 0, 2);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        toBytes(s, bytes, off, 2);

        return off + 2;
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link java.util.UUID}.
     */
    public static byte[] uuidToBytes(@Nullable UUID uuid) {
        byte[] bytes = new byte[16];

        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, bytes.length);

        buf.order(ByteOrder.BIG_ENDIAN);

        if (uuid != null) {
            buf.putLong(uuid.getMostSignificantBits());
            buf.putLong(uuid.getLeastSignificantBits());
        }
        else {
            buf.putLong(0);
            buf.putLong(0);
        }

        return bytes;
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        return (short)fromBytes(bytes, off, 2);
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        return (int)fromBytes(bytes, off, 4);
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        return fromBytes(bytes, off, 8);
    }

    /**
     * Constructs {@link UUID} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        long most = buf.getLong();
        long least = buf.getLong();

        return most != 0 && least != 0 ? new UUID(most, least) : null;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array. The highest byte in the value is the first byte in result array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @param limit Limit of bytes to write into output.
     * @return Array of bytes.
     */
    private static byte[] toBytes(long l, byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        for (int i = limit - 1; i >= 0; i--) {
            bytes[off + i] = (byte)(l & 0xFF);
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Constructs {@code long} from byte array. The first byte in array is the highest byte in result.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @param limit Amount of bytes to use in the source array.
     * @return Long value.
     */
    private static long fromBytes(byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;

        int bytesCnt = Math.min(bytes.length - off, limit);

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            res <<= 8;
            res |= bytes[off + i] & 0xFF;
        }

        return res;
    }

    /**
     * Compares fragments of byte arrays.
     *
     * @param a First array.
     * @param aOff First array offset.
     * @param b Second array.
     * @param bOff Second array offset.
     * @param len Length of fragments.
     * @return {@code true} if fragments are equal, {@code false} otherwise.
     */
    public static boolean bytesEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
        if (aOff + len > a.length || bOff + len > b.length)
            return false;
        else {
            for (int i = 0; i < len; i++)
                if (a[aOff + i] != b[bOff + i])
                    return false;

            return true;
        }
    }

    /**
     * Converts an array of characters representing hexidecimal values into an
     * array of bytes of those same values. The returned array will be half the
     * length of the passed array, as it takes two characters to represent any
     * given byte. An exception is thrown if the passed char array has an odd
     * number of elements.
     *
     * @param data An array of characters containing hexidecimal digits
     * @return A byte array containing binary data decoded from
     *         the supplied char array.
     * @throws IgniteCheckedException Thrown if an odd number or illegal of characters is supplied.
     */
    public static byte[] decodeHex(char[] data) throws IgniteCheckedException {
        int len = data.length;

        if ((len & 0x01) != 0)
            throw new IgniteCheckedException("Odd number of characters.");

        byte[] out = new byte[len >> 1];

        // Two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;

            j++;

            f |= toDigit(data[j], j);

            j++;

            out[i] = (byte)(f & 0xFF);
        }

        return out;
    }

    /**
     * @param bytes Number of bytes to display.
     * @param si If {@code true}, then unit base is 1000, otherwise unit base is 1024.
     * @return Formatted size.
     */
    public static String readableSize(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;

        if (bytes < unit)
            return bytes + " B";

        int exp = (int)(Math.log(bytes) / Math.log(unit));

        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Generates file name from index.
     *
     * @param num Number to generate file name.
     * @param ext Optional extension
     * @return File name.
     */
    public static String fixedLengthNumberName(long num, @Nullable String ext) {
        SB b = new SB();

        String segmentStr = Long.toString(num);

        for (int i = segmentStr.length(); i < NUMBER_FILE_NAME_LENGTH; i++)
            b.a('0');

        b.a(segmentStr);

        if (ext != null)
            b.a(ext);

        return b.toString();
    }

    /**
     * @param fileName File name.
     * @return Number of this file.
     */
    public static long fixedLengthFileNumber(String fileName) {
        return Long.parseLong(fileName.substring(0, NUMBER_FILE_NAME_LENGTH));
    }

    /**
     * @param ext Optional extension.
     * @return Pattern to match numbered file name with the specific extension.
     */
    public static Pattern fixedLengthNumberNamePattern(@Nullable String ext) {
        String pattern = "\\d{" + NUMBER_FILE_NAME_LENGTH + "}";

        if (ext != null)
            pattern += ext.replaceAll("\\.", "\\\\\\.");

        return Pattern.compile(pattern);
    }

    /**
     * Makes a {@code '+---+'} dash line.
     *
     * @param len Length of the dash line to make.
     * @return Dash line.
     */
    public static String dash(int len) {
        char[] dash = new char[len];

        Arrays.fill(dash, '-');

        dash[0] = dash[len - 1] = '+';

        return new String(dash);
    }

    /**
     * Formats system time in milliseconds for printing in logs.
     *
     * @param sysTime System time.
     * @return Formatted time string.
     */
    public static String format(long sysTime) {
        return LONG_DATE_FMT.format(Instant.ofEpochMilli(sysTime));
    }

    /**
     * Converts enumeration to iterable so it can be used in {@code foreach} construct.
     *
     * @param <T> Types of instances for iteration.
     * @param e Enumeration to convert.
     * @return Iterable over the given enumeration.
     */
    public static <T> Iterable<T> asIterable(final Enumeration<T> e) {
        return new Iterable<T>() {
            @Override public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override public boolean hasNext() {
                        return e.hasMoreElements();
                    }

                    @Override public T next() {
                        return e.nextElement();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Copy source file (or folder) to destination file (or folder). Supported source & destination:
     * <ul>
     * <li>File to File</li>
     * <li>File to Folder</li>
     * <li>Folder to Folder (Copy the content of the directory and not the directory itself)</li>
     * </ul>
     *
     * @param src Source file or folder.
     * @param dest Destination file or folder.
     * @param overwrite Whether or not overwrite existing files and folders.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void copy(File src, File dest, boolean overwrite) throws IOException {
        assert src != null;
        assert dest != null;

        /*
         * Supported source & destination:
         * ===============================
         * 1. File -> File
         * 2. File -> Directory
         * 3. Directory -> Directory
         */

        // Source must exist.
        if (!src.exists())
            throw new FileNotFoundException("Source can't be found: " + src);

        // Check that source and destination are not the same.
        if (src.getAbsoluteFile().equals(dest.getAbsoluteFile()))
            throw new IOException("Source and destination are the same [src=" + src + ", dest=" + dest + ']');

        if (dest.exists()) {
            if (!dest.isDirectory() && !overwrite)
                throw new IOException("Destination already exists: " + dest);

            if (!dest.canWrite())
                throw new IOException("Destination is not writable:" + dest);
        }
        else {
            File parent = dest.getParentFile();

            if (parent != null && !parent.exists())
                // Ignore any errors here.
                // We will get errors when we'll try to open the file stream.
                //noinspection ResultOfMethodCallIgnored
                parent.mkdirs();

            // If source is a directory, we should create destination directory.
            if (src.isDirectory())
                //noinspection ResultOfMethodCallIgnored
                dest.mkdir();
        }

        if (src.isDirectory()) {
            // In this case we have Directory -> Directory.
            // Note that we copy the content of the directory and not the directory itself.

            File[] files = src.listFiles();

            for (File file : files) {
                if (file.isDirectory()) {
                    File dir = new File(dest, file.getName());

                    if (!dir.exists() && !dir.mkdirs())
                        throw new IOException("Can't create directory: " + dir);

                    copy(file, dir, overwrite);
                }
                else
                    copy(file, dest, overwrite);
            }
        }
        else {
            // In this case we have File -> File or File -> Directory.
            File file = dest.exists() && dest.isDirectory() ? new File(dest, src.getName()) : dest;

            if (!overwrite && file.exists())
                throw new IOException("Destination already exists: " + file);

            FileInputStream in = null;
            FileOutputStream out = null;

            try {
                in = new FileInputStream(src);
                out = new FileOutputStream(file);

                copy(in, out);
            }
            finally {
                if (in != null)
                    in.close();

                if (out != null) {
                    out.getFD().sync();

                    out.close();
                }
            }
        }
    }

    /**
     * Starts clock timer if grid is first.
     */
    public static void onGridStart() {
        synchronized (mux) {
            if (gridCnt == 0) {
                assert timer == null;

                timer = new Thread(new Runnable() {
                    @SuppressWarnings({"BusyWait"})
                    @Override public void run() {
                        while (true) {
                            curTimeMillis = System.currentTimeMillis();

                            try {
                                Thread.sleep(10);
                            }
                            catch (InterruptedException ignored) {
                                break;
                            }
                        }
                    }
                }, "ignite-clock");

                timer.setDaemon(true);

                timer.setPriority(10);

                timer.start();
            }

            ++gridCnt;
        }
    }

    /**
     * Stops clock timer if all nodes into JVM were stopped.
     * @throws InterruptedException If interrupted.
     */
    public static void onGridStop() throws InterruptedException {
        synchronized (mux) {
            // Grid start may fail and onGridStart() does not get called.
            if (gridCnt == 0)
                return;

            --gridCnt;

            Thread timer0 = timer;

            if (gridCnt == 0 && timer0 != null) {
                timer = null;

                timer0.interrupt();

                timer0.join();
            }
        }
    }

    /**
     * Copies input byte stream to output byte stream.
     *
     * @param in Input byte stream.
     * @param out Output byte stream.
     * @return Number of the copied bytes.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        assert in != null;
        assert out != null;

        byte[] buf = new byte[BUF_SIZE];

        int cnt = 0;

        for (int n; (n = in.read(buf)) > 0; ) {
            out.write(buf, 0, n);

            cnt += n;
        }

        return cnt;
    }

    /**
     * Reads file to string using specified charset.
     *
     * @param fileName File name.
     * @param charset File charset.
     * @return File content.
     * @throws IOException If error occurred.
     */
    public static String readFileToString(String fileName, String charset) throws IOException {
        try (Reader input = new InputStreamReader(new FileInputStream(fileName), charset)) {
            StringWriter output = new StringWriter();

            char[] buf = new char[4096];

            int n;

            while ((n = input.read(buf)) != -1)
                output.write(buf, 0, n);

            return output.toString();
        }
    }

    /**
     * Writes string to file.
     *
     * @param file File.
     * @param s String to write.
     * @param charset Encoding.
     * @param append If {@code true}, then specified string will be added to the end of the file.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void writeStringToFile(File file, String s, String charset, boolean append) throws IOException {
        if (s == null)
            return;

        try (OutputStream out = new FileOutputStream(file, append)) {
            out.write(s.getBytes(charset));
        }
    }

    /**
     * Utility method that sets cause into exception and returns it.
     *
     * @param e Exception to set cause to and return.
     * @param cause Optional cause to set (if not {@code null}).
     * @param <E> Type of the exception.
     * @return Passed in exception with optionally set cause.
     */
    public static <E extends Throwable> E withCause(E e, @Nullable Throwable cause) {
        assert e != null;

        if (cause != null)
            e.initCause(cause);

        return e;
    }

    /**
     * Deletes file or directory with all sub-directories and files. Not thread-safe.
     *
     * @param file File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(@Nullable File file) {
        return file != null && delete(file.toPath());
    }

    /**
     * Converts size in bytes to human-readable size in megabytes.
     *
     * @param sizeInBytes Size of any object (file, memory region etc) in bytes.
     * @return Size converted to megabytes.
     */
    public static int sizeInMegabytes(long sizeInBytes) {
        return (int)(sizeInBytes / MB);
    }

    /**
     * Deletes file or directory with all sub-directories and files. Not thread-safe.
     *
     * @param path File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(Path path) {
        if (Files.isDirectory(path)) {
            try {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path innerPath : stream) {
                        boolean res = delete(innerPath);

                        if (!res)
                            return false;
                    }
                }
            }
            catch (IOException e) {
                return false;
            }
        }

        if (path.toFile().getName().endsWith("jar")) {
            try {
                // Why do we do this?
                new JarFile(path.toString(), false).close();
            }
            catch (IOException ignore) {
                // Ignore it here...
            }
        }

        try {
            Files.delete(path);

            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /**
     * @param dir Directory to create along with all non-existent parent directories.
     * @return {@code True} if directory exists (has been created or already existed),
     *      {@code false} if has not been created and does not exist.
     */
    public static boolean mkdirs(File dir) {
        assert dir != null;

        return dir.mkdirs() || dir.exists();
    }

    /**
     * Resolve project home directory based on source code base.
     *
     * @return Project home directory (or {@code null} if it cannot be resolved).
     */
    @Nullable private static String resolveProjectHome() {
        assert Thread.holdsLock(IgniteUtils.class);

        // Resolve Ignite home via environment variables.
        String ggHome0 = IgniteSystemProperties.getString(IGNITE_HOME);

        if (!F.isEmpty(ggHome0))
            return ggHome0;

        String appWorkDir = System.getProperty("user.dir");

        if (appWorkDir != null) {
            ggHome0 = findProjectHome(new File(appWorkDir));

            if (ggHome0 != null)
                return ggHome0;
        }

        URI classesUri;

        Class<IgniteUtils> cls = IgniteUtils.class;

        try {
            ProtectionDomain domain = cls.getProtectionDomain();

            // Should not happen, but to make sure our code is not broken.
            if (domain == null || domain.getCodeSource() == null || domain.getCodeSource().getLocation() == null) {
                logResolveFailed(cls, null);

                return null;
            }

            // Resolve path to class-file.
            classesUri = domain.getCodeSource().getLocation().toURI();

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (isWindows() && classesUri.getAuthority() != null)
                classesUri = new URI(classesUri.toString().replace("file://", "file:/"));
        }
        catch (URISyntaxException | SecurityException e) {
            logResolveFailed(cls, e);

            return null;
        }

        File classesFile;

        try {
            classesFile = new File(classesUri);
        }
        catch (IllegalArgumentException e) {
            logResolveFailed(cls, e);

            return null;
        }

        return findProjectHome(classesFile);
    }

    /**
     * Tries to find project home starting from specified directory and moving to root.
     *
     * @param startDir First directory in search hierarchy.
     * @return Project home path or {@code null} if it wasn't found.
     */
    private static String findProjectHome(File startDir) {
        for (File cur = startDir.getAbsoluteFile(); cur != null; cur = cur.getParentFile()) {
            // Check 'cur' is project home directory.
            if (!new File(cur, "bin").isDirectory() ||
                !new File(cur, "config").isDirectory())
                continue;

            return cur.getPath();
        }

        return null;
    }

    /**
     * @param cls Class.
     * @param e Exception.
     */
    private static void logResolveFailed(Class cls, Exception e) {
        warn(null, "Failed to resolve IGNITE_HOME automatically for class codebase " +
            "[class=" + cls + (e == null ? "" : ", e=" + e.getMessage()) + ']');
    }

    /**
     * Retrieves {@code IGNITE_HOME} property. The property is retrieved from system
     * properties or from environment in that order.
     *
     * @return {@code IGNITE_HOME} property.
     */
    @Nullable public static String getIgniteHome() {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (IgniteUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    // Resolve Ignite installation home directory.
                    ggHome = F.t(ggHome0 = resolveProjectHome());

                    if (ggHome0 != null)
                        System.setProperty(IGNITE_HOME, ggHome0);
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        return ggHome0;
    }

    /**
     * @param path Ignite home. May be {@code null}.
     */
    public static void setIgniteHome(@Nullable String path) {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (IgniteUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    if (F.isEmpty(path))
                        System.clearProperty(IGNITE_HOME);
                    else
                        System.setProperty(IGNITE_HOME, path);

                    ggHome = F.t(path);

                    return;
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        if (ggHome0 != null && !ggHome0.equals(path)) {
            try {
                Path path0 = new File(ggHome0).toPath();

                Path path1 = new File(path).toPath();

                if (!Files.isSameFile(path0, path1))
                    throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                        "[igniteHome=" + path0 + ", newIgniteHome=" + path1 + ']');
            }
            catch (IOException ignore) {
                // Throw an exception if failed to follow symlinks.
                throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                    "[igniteHome=" + ggHome0 + ", newIgniteHome=" + path + ']');
            }
        }
    }

    /**
     * Gets file associated with path.
     * <p>
     * First check if path is relative to {@code IGNITE_HOME}.
     * If not, check if path is absolute.
     * If all checks fail, then {@code null} is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path as file, or {@code null} if path cannot be resolved.
     */
    @Nullable public static File resolveIgnitePath(String path) {
        assert path != null;

        /*
         * 1. Check relative to IGNITE_HOME specified in configuration, if any.
         */

        String home = getIgniteHome();

        if (home != null) {
            File file = new File(home, path);

            if (file.exists())
                return file;
        }

        /*
         * 2. Check given path as absolute.
         */

        File file = new File(path);

        if (file.exists())
            return file;

        return null;
    }

    /**
     * Gets URL representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to {@code META-INF} folder in classpath.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}.
     * If all checks fail,
     * then {@code null} is returned, otherwise URL representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path as URL, or {@code null} if path cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static URL resolveIgniteUrl(String path) {
        return resolveIgniteUrl(path, true);
    }

    /**
     * Resolve Spring configuration URL.
     *
     * @param springCfgPath Spring XML configuration file path or URL. This cannot be {@code null}.
     * @return URL.
     * @throws IgniteCheckedException If failed.
     */
    public static URL resolveSpringUrl(String springCfgPath) throws IgniteCheckedException {
        A.notNull(springCfgPath, "springCfgPath");

        URL url;

        try {
            url = new URL(springCfgPath);
        }
        catch (MalformedURLException e) {
            url = resolveIgniteUrl(springCfgPath);

            if (url == null)
                url = resolveInClasspath(springCfgPath);

            if (url == null)
                throw new IgniteCheckedException("Spring XML configuration path is invalid: " + springCfgPath +
                    ". Note that this path should be either absolute or a relative local file system path, " +
                    "relative to META-INF in classpath or valid URL to IGNITE_HOME.", e);
        }

        return url;
    }

    /**
     * @param path Resource path.
     * @return Resource URL inside classpath or {@code null}.
     */
    @Nullable private static URL resolveInClasspath(String path) {
        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        if (clsLdr == null)
            return null;

        return clsLdr.getResource(path.replaceAll("\\\\", "/"));
    }

    /**
     * Gets URL representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to {@code META-INF} folder in classpath.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}.
     * If all checks fail,
     * then {@code null} is returned, otherwise URL representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @param metaInf Flag to indicate whether META-INF folder should be checked or class path root.
     * @return Resolved path as URL, or {@code null} if path cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static URL resolveIgniteUrl(String path, boolean metaInf) {
        File f = resolveIgnitePath(path);

        if (f != null) {
            try {
                // Note: we use that method's chain instead of File.getURL() with due
                // Sun bug http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179468
                return f.toURI().toURL();
            }
            catch (MalformedURLException e) {
                // No-op.
            }
        }

        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        if (clsLdr != null) {
            String locPath = (metaInf ? "META-INF/" : "") + path.replaceAll("\\\\", "/");

            return clsLdr.getResource(locPath);
        }
        else
            return null;
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String byteArray2HexString(byte[] arr) {
        return byteArray2HexString(arr, true);
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @param toUpper If {@code true} returns upper cased result.
     * @return Hex string.
     */
    public static String byteArray2HexString(byte[] arr, boolean toUpper) {
        StringBuilder sb = new StringBuilder(arr.length << 1);

        for (byte b : arr)
            addByteAsHex(sb, b);

        return toUpper ? sb.toString().toUpperCase() : sb.toString();
    }

    /**
     * @param sb String builder.
     * @param b Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
    }

    /**
     * Checks for containment of the value in the array.
     * Both array cells and value may be {@code null}. Two {@code null}s are considered equal.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @param vals Additional values.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsObjectArray(@Nullable Object[] arr, Object val, @Nullable Object... vals) {
        if (arr == null || arr.length == 0)
            return false;

        for (Object o : arr) {
            if (Objects.equals(o, val))
                return true;

            if (vals != null && vals.length > 0)
                for (Object v : vals)
                    if (Objects.equals(o, v))
                        return true;
        }

        return false;
    }

    /**
     * Checks for containment of the value in the array.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsIntArray(int[] arr, int val) {
        assert arr != null;

        if (arr.length == 0)
            return false;

        for (int i : arr)
            if (i == val)
                return true;

        return false;
    }

    /**
     * Checks for containment of given string value in the specified array.
     * Array's cells and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param arr Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringArray(String[] arr, @Nullable String val, boolean ignoreCase) {
        assert arr != null;

        for (String s : arr) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Checks for containment of given string value in the specified collection.
     * Collection elements and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param c Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringCollection(Iterable<String> c, @Nullable String val, boolean ignoreCase) {
        assert c != null;

        for (String s : c) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable AutoCloseable rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null) {
            try {
                rsrc.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Closes given socket logging possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Socket sock, @Nullable IgniteLogger log) {
        if (sock == null || sock.isClosed())
            return;

        try {
            // Closing output and input first to avoid tls 1.3 incompatibility
            // https://bugs.openjdk.java.net/browse/JDK-8208526
            if (!sock.isOutputShutdown())
                sock.shutdownOutput();
            if (!sock.isInputShutdown())
                sock.shutdownInput();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to shutdown socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to shutdown socket: " + e.getMessage(), e);
        }

        try {
            sock.close();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to close socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to close socket: " + e.getMessage(), e);
        }
    }

    /**
     * Closes given resource suppressing possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param e Suppressor exception
     */
    public static void closeWithSuppressingException(@Nullable AutoCloseable rsrc, @NotNull Exception e) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
    }

    /**
     * Quietly closes given resource ignoring possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable AutoCloseable rsrc) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception ignored) {
                // No-op.
            }
    }

    /**
     * Closes given resource logging possible checked exceptions.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable SelectionKey rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            // This apply will automatically deregister the selection key as well.
            close(rsrc.channel(), log);
    }

    /**
     * Closes given resource.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void close(@Nullable DatagramSocket rsrc) {
        if (rsrc != null)
            rsrc.close();
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Selector rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                if (rsrc.isOpen())
                    rsrc.close();
            }
            catch (IOException e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
    }

    /**
     * Closes class loader logging possible checked exception.
     *
     * @param clsLdr Class loader. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable URLClassLoader clsLdr, @Nullable IgniteLogger log) {
        if (clsLdr != null) {
            try {
                clsLdr.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
        }
    }

    /**
     * Quietly closes given {@link Socket} ignoring possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Socket sock) {
        if (sock == null)
            return;

        try {
            // Avoid tls 1.3 incompatibility https://bugs.openjdk.java.net/browse/JDK-8208526
            sock.shutdownOutput();
            sock.shutdownInput();
        }
        catch (Exception ignored) {
            // No-op.
        }

        try {
            sock.close();
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /**
     * Quietly releases file lock ignoring all possible exceptions.
     *
     * @param lock File lock. If it's {@code null} - it's no-op.
     */
    public static void releaseQuiet(@Nullable FileLock lock) {
        if (lock != null)
            try {
                lock.release();
            }
            catch (Exception ignored) {
                // No-op.
            }
    }

    /**
     * Rollbacks JDBC connection logging possible checked exception.
     *
     * @param rsrc JDBC connection to rollback. If connection is {@code null}, it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void rollbackConnection(@Nullable Connection rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                rsrc.rollback();
            }
            catch (SQLException e) {
                warn(log, "Failed to rollback JDBC connection: " + e.getMessage());
            }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given messages as
     * quiet message or normal log WARN message in {@code org.apache.ignite.CourtesyConfigNotice}
     * category. If {@code log} is {@code null} or in QUIET mode it will add {@code (courtesy)}
     * prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void courtesy(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        courtesy(log, s, s);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given messages as
     * quiet message or normal log WARN message in {@code org.apache.ignite.CourtesyConfigNotice}
     * category. If {@code log} is {@code null} or in QUIET mode it will add {@code (courtesy)}
     * prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     */
    public static void courtesy(@Nullable IgniteLogger log, Object longMsg, Object shortMsg) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null)
            log.getLogger(IgniteConfiguration.COURTESY_LOGGER_NAME).warning(compact(longMsg.toString()));
        else
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (courtesy) " +
                compact(shortMsg.toString()));
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        warn(log, s, null);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg) {
        quietAndWarn(log, msg, msg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param shortMsg Short message.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, Object shortMsg) {
        warn(log, msg);

        if (log.isQuiet())
            quiet(false, shortMsg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     * @param e Optional exception.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, @Nullable Throwable e) {
        warn(log, msg, e);

        if (log.isQuiet())
            quiet(false, msg);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void error(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        if (msg instanceof Throwable) {
            Throwable t = (Throwable)msg;

            error(log, t.getMessage(), t);
        }
        else {
            String s = msg.toString();

            error(log, s, s, null);
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log using normal logger.
     * @param e Optional exception.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg, @Nullable Throwable e) {
        assert msg != null;

        if (log != null)
            log.warning(compact(msg.toString()), e);
        else {
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                    compact(msg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message with {@link IgniteLogger#DEV_ONLY DEV_ONLY} marker.
     * If {@code log} is {@code null} or in QUIET mode it will add {@code (wrn)} prefix to the message.
     * If property {@link IgniteSystemProperties#IGNITE_DEV_ONLY_LOGGING_DISABLED IGNITE_DEV_ONLY_LOGGING_DISABLED}
     * is set to true, the message will not be logged.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warnDevOnly(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        // don't log message if DEV_ONLY messages are disabled
        if (devOnlyLogDisabled)
            return;

        if (log != null)
            log.warning(IgniteLogger.DEV_ONLY, compact(msg.toString()), null);
        else
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                compact(msg.toString()));
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INFO message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     */
    public static void log(@Nullable IgniteLogger log, Object longMsg, Object shortMsg) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (log.isInfoEnabled())
                log.info(compact(longMsg.toString()));
        }
        else
            quiet(false, shortMsg);
    }

    /**
     * Resolves work directory.
     * @param cfg Ignite configuration.
     */
    public static void initWorkDir(IgniteConfiguration cfg) throws IgniteCheckedException {
        String igniteHome = cfg.getIgniteHome();

        // Set Ignite home.
        if (igniteHome == null)
            igniteHome = getIgniteHome();

        String userProvidedWorkDir = cfg.getWorkDirectory();

        // Correctly resolve work directory and set it back to configuration.
        cfg.setWorkDirectory(workDirectory(userProvidedWorkDir, igniteHome));
    }

    /**
     * @param cfg Ignite configuration.
     * @param app Application name.
     * @return Initialized logger.
     * @throws IgniteCheckedException If failed.
     */
    public static IgniteLogger initLogger(IgniteConfiguration cfg, String app) throws IgniteCheckedException {
        return initLogger(
            cfg.getGridLogger(),
            app,
            null,
            cfg.getWorkDirectory()
        );
    }

    /**
     * @param cfgLog Configured logger.
     * @param app Application name.
     * @param workDir Work directory.
     * @return Initialized logger.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public static IgniteLogger initLogger(
        @Nullable IgniteLogger cfgLog,
        @Nullable String app,
        @Nullable UUID nodeId,
        String workDir
    ) throws IgniteCheckedException {
        try {
            Exception log4jInitErr = null;

            if (cfgLog == null) {
                Class<?> log4jCls;

                try {
                    log4jCls = Class.forName("org.apache.ignite.logger.log4j2.Log4J2Logger");
                }
                catch (ClassNotFoundException | NoClassDefFoundError ignored) {
                    log4jCls = null;
                }

                if (log4jCls != null) {
                    try {
                        URL url = resolveIgniteUrl("config/ignite-log4j.xml");

                        if (url == null) {
                            File cfgFile = new File("config/ignite-log4j.xml");

                            if (!cfgFile.exists())
                                cfgFile = new File("../config/ignite-log4j.xml");

                            if (cfgFile.exists()) {
                                try {
                                    url = cfgFile.toURI().toURL();
                                }
                                catch (MalformedURLException ignore) {
                                    // No-op.
                                }
                            }
                        }

                        if (url != null) {
                            boolean configured = (Boolean)log4jCls.getMethod("isConfigured").invoke(null);

                            if (configured)
                                url = null;
                        }

                        if (url != null) {
                            Constructor<?> ctor = log4jCls.getConstructor(URL.class);

                            cfgLog = (IgniteLogger)ctor.newInstance(url);
                        }
                        else
                            cfgLog = (IgniteLogger)log4jCls.newInstance();
                    }
                    catch (Exception e) {
                        log4jInitErr = e;
                    }
                }

                if (log4jCls == null || log4jInitErr != null)
                    cfgLog = new JavaLogger();
            }

            // Special handling for Java logger which requires work directory.
            if (cfgLog instanceof JavaLogger)
                ((JavaLogger)cfgLog).setWorkDirectory(workDir);

            // Set node IDs for all file appenders.
            if (cfgLog instanceof IgniteLoggerEx)
                ((IgniteLoggerEx)cfgLog).setApplicationAndNode(app, nodeId);

            if (log4jInitErr != null)
                warn(cfgLog, "Failed to initialize Log4J2Logger (falling back to standard java logging): "
                    + log4jInitErr.getCause());

            return cfgLog;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to create logger.", e);
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INF0 message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void log(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        log(log, s, s);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object longMsg, Object shortMsg, @Nullable Throwable e) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (e == null)
                log.error(compact(longMsg.toString()));
            else
                log.error(compact(longMsg.toString()), e);
        }
        else {
            X.printerr("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (err) " +
                compact(shortMsg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Shortcut for {@link #error(org.apache.ignite.IgniteLogger, Object, Object, Throwable)}.
     *
     * @param log Optional logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object shortMsg, @Nullable Throwable e) {
        assert shortMsg != null;

        String s = shortMsg.toString();

        error(log, s, s, e);
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param objs Objects to log in quiet mode.
     */
    public static void quiet(boolean err, Object... objs) {
        assert objs != null;

        String time = SHORT_DATE_FMT.format(Instant.now());

        SB sb = new SB();

        for (Object obj : objs)
            sb.a('[').a(time).a("] ").a(obj.toString()).a(NL);

        PrintStream ps = err ? System.err : System.out;

        ps.print(compact(sb.toString()));
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param multiline Multiple lines string to print.
     */
    public static void quietMultipleLines(boolean err, String multiline) {
        assert multiline != null;

        quiet(err, multiline.split(NL));
    }

    /**
     * Prints out the message in quiet and info modes.
     *
     * @param log Logger.
     * @param msg Message to print.
     */
    public static void quietAndInfo(IgniteLogger log, String msg) {
        if (log.isQuiet())
            quiet(false, msg);

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     * Quietly rollbacks JDBC connection ignoring possible checked exception.
     *
     * @param rsrc JDBC connection to rollback. If connection is {@code null}, it's no-op.
     */
    public static void rollbackConnectionQuiet(@Nullable Connection rsrc) {
        if (rsrc != null)
            try {
                rsrc.rollback();
            }
            catch (SQLException ignored) {
                // No-op.
            }
    }

    /**
     * Constructs JMX object name with given properties.
     * Map with ordered {@code groups} used for proper object name construction.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param grp Name of the group.
     * @param name Name of mbean.
     * @return JMX object name.
     * @throws MalformedObjectNameException Thrown in case of any errors.
     */
    public static ObjectName makeMBeanName(@Nullable String igniteInstanceName, @Nullable String grp, String name)
        throws MalformedObjectNameException {
        return makeMBeanName(igniteInstanceName, grp, Collections.emptyList(), name);
    }

    /**
     * Constructs JMX object name with given properties.
     * Map with ordered {@code groups} used for proper object name construction.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param grp Name of the group.
     * @param grps Names of extended groups.
     * @param name Name of mbean.
     * @return JMX object name.
     * @throws MalformedObjectNameException Thrown in case of any errors.
     */
    public static ObjectName makeMBeanName(
        @Nullable String igniteInstanceName,
        @Nullable String grp,
        List<String> grps,
        String name
    ) throws MalformedObjectNameException {
        SB sb = new SB(JMX_DOMAIN + ':');

        appendClassLoaderHash(sb);

        appendJvmId(sb);

        if (igniteInstanceName != null && !igniteInstanceName.isEmpty())
            sb.a("igniteInstanceName=").a(igniteInstanceName).a(',');

        if (grp != null)
            sb.a("group=").a(escapeObjectNameValue(grp)).a(',');

        for (int i = 0; i < grps.size(); i++)
            sb.a("group").a(i).a("=").a(grps.get(i)).a(',');

        sb.a("name=").a(escapeObjectNameValue(name));

        return new ObjectName(sb.toString());
    }

    /**
     * @param sb Sb.
     */
    private static void appendClassLoaderHash(SB sb) {
        if (getBoolean(IGNITE_MBEAN_APPEND_CLASS_LOADER_ID, DFLT_MBEAN_APPEND_CLASS_LOADER_ID)) {
            String clsLdrHash = Integer.toHexString(Ignite.class.getClassLoader().hashCode());

            sb.a("clsLdr=").a(clsLdrHash).a(',');
        }
    }

    /**
     * @param sb Sb.
     */
    private static void appendJvmId(SB sb) {
        if (getBoolean(IGNITE_MBEAN_APPEND_JVM_ID)) {
            String jvmId = ManagementFactory.getRuntimeMXBean().getName();

            sb.a("jvmId=").a(jvmId).a(',');
        }
    }

    /**
     * Mask component name to make sure that it is not {@code null}.
     *
     * @param name Component name to mask, possibly {@code null}.
     * @return Component name.
     */
    public static String maskName(@Nullable String name) {
        return name == null ? "default" : name;
    }

    /**
     * Escapes the given string to be used as a value in the ObjectName syntax.
     *
     * @param s A string to be escape.
     * @return An escaped string.
     */
    private static String escapeObjectNameValue(String s) {
        if (alphanumericUnderscore(s))
            return s;

        return '\"' + s.replaceAll("[\\\\\"?*]", "\\\\$0") + '\"';
    }

    /**
     * @param s String to check.
     * @return {@code true} if given string contains only alphanumeric and underscore symbols.
     */
    public static boolean alphanumericUnderscore(String s) {
        return ALPHANUMERIC_UNDERSCORE_PATTERN.matcher(s).matches();
    }

    /**
     * Registers MBean with the server.
     *
     * @param <T> Type of mbean.
     * @param mbeanSrv MBean server.
     * @param igniteInstanceName Ignite instance name.
     * @param grp Name of the group.
     * @param name Name of mbean.
     * @param impl MBean implementation.
     * @param itf MBean interface.
     * @return JMX object name.
     * @throws MBeanRegistrationException if MBeans are disabled.
     * @throws JMException If MBean creation failed.
     */
    public static <T> ObjectName registerMBean(
        MBeanServer mbeanSrv,
        @Nullable String igniteInstanceName,
        @Nullable String grp,
        String name, T impl,
        @Nullable Class<T> itf
    ) throws JMException {
        return registerMBean(mbeanSrv, makeMBeanName(igniteInstanceName, grp, name), impl, itf);
    }

    /**
     * Registers MBean with the server.
     *
     * @param <T> Type of mbean.
     * @param mbeanSrv MBean server.
     * @param name MBean object name.
     * @param impl MBean implementation.
     * @param itf MBean interface.
     * @return JMX object name.
     * @throws MBeanRegistrationException if MBeans are disabled.
     * @throws JMException If MBean creation failed.
     * @throws IgniteException If MBean creation are not allowed.
     */
    public static <T> ObjectName registerMBean(MBeanServer mbeanSrv, ObjectName name, T impl, Class<T> itf)
        throws JMException {
        if (IGNITE_MBEANS_DISABLED)
            throw new MBeanRegistrationException(new IgniteIllegalStateException("MBeans are disabled."));

        assert mbeanSrv != null;
        assert name != null;
        assert itf != null;

        DynamicMBean mbean;

        if (impl instanceof DynamicMBean) {
            mbean = (DynamicMBean)impl;
        }
        else {
            mbean = new IgniteStandardMXBean(impl, itf);

            mbean.getMBeanInfo();
        }

        return mbeanSrv.registerMBean(mbean, name).getObjectName();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param t Thread to interrupt.
     */
    public static void interrupt(@Nullable Thread t) {
        if (t != null)
            t.interrupt();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param workers Threads to interrupt.
     */
    public static void interrupt(Iterable<? extends Thread> workers) {
        if (workers != null)
            for (Thread worker : workers)
                worker.interrupt();
    }

    /**
     * Waits for completion of a given thread. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param t Thread to join.
     * @param log Logger for logging errors.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean join(@Nullable Thread t, @Nullable IgniteLogger log) {
        return join(t, log, 0);
    }

    /**
     * Waits for completion of a given thread. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param t Thread to join.
     * @param log Logger for logging errors.
     * @param timeout Join timeout.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean join(@Nullable Thread t, @Nullable IgniteLogger log, long timeout) {
        if (t != null) {
            try {
                t.join(timeout);

                return !t.isAlive();
            }
            catch (InterruptedException ignore) {
                warn(log, "Got interrupted while waiting for completion of a thread: " + t);

                Thread.currentThread().interrupt();

                return false;
            }
        }

        return true;
    }

    /**
     * Waits for completion of a given threads. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param workers Thread to join.
     * @param log Logger for logging errors.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean joinThreads(Iterable<? extends Thread> workers, @Nullable IgniteLogger log) {
        boolean retval = true;

        if (workers != null)
            for (Thread worker : workers)
                if (!join(worker, log))
                    retval = false;

        return retval;
    }

    /**
     * Cancels given runnable.
     *
     * @param w Worker to cancel - it's no-op if runnable is {@code null}.
     */
    public static void cancel(@Nullable GridWorker w) {
        if (w != null)
            w.cancel();
    }

    /**
     * Cancels collection of runnables.
     *
     * @param ws Collection of workers - it's no-op if collection is {@code null}.
     */
    public static void cancel(Iterable<? extends GridWorker> ws) {
        if (ws != null)
            for (GridWorker w : ws)
                w.cancel();
    }

    /**
     * Joins runnable.
     *
     * @param w Worker to join.
     * @param log The logger to possible exception.
     * @return {@code true} if worker has not been interrupted, {@code false} if it was interrupted.
     */
    public static boolean join(@Nullable GridWorker w, @Nullable IgniteLogger log) {
        if (w != null)
            try {
                w.join();
            }
            catch (InterruptedException ignore) {
                warn(log, "Got interrupted while waiting for completion of runnable: " + w);

                Thread.currentThread().interrupt();

                return false;
            }

        return true;
    }

    /**
     * Joins given collection of runnables.
     *
     * @param ws Collection of workers to join.
     * @param log The logger to possible exceptions.
     * @return {@code true} if none of the worker have been interrupted,
     *      {@code false} if at least one was interrupted.
     */
    public static boolean join(Iterable<? extends GridWorker> ws, IgniteLogger log) {
        boolean retval = true;

        if (ws != null)
            for (GridWorker w : ws)
                if (!join(w, log))
                    retval = false;

        return retval;
    }

    /**
     * Shutdowns given {@code ExecutorService} and wait for executor service to stop.
     *
     * @param owner The ExecutorService owner.
     * @param exec ExecutorService to shutdown.
     * @param log The logger to possible exceptions and warnings.
     */
    public static void shutdownNow(Class<?> owner, @Nullable ExecutorService exec, @Nullable IgniteLogger log) {
        if (exec != null) {
            List<Runnable> tasks = exec.shutdownNow();

            if (!F.isEmpty(tasks))
                warn(log, "Runnable tasks outlived thread pool executor service [owner=" + getSimpleName(owner) +
                    ", tasks=" + tasks + ']');

            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                warn(log, "Got interrupted while waiting for executor service to stop.");

                exec.shutdownNow();

                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Creates appropriate empty projection exception.
     *
     * @return Empty projection exception.
     */
    public static ClusterGroupEmptyCheckedException emptyTopologyException() {
        return new ClusterGroupEmptyCheckedException("Cluster group is empty.");
    }

    /**
     * Writes UUID to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeUuid(DataOutput out, UUID uid) throws IOException {
        // Write null flag.
        out.writeBoolean(uid == null);

        if (uid != null) {
            out.writeLong(uid.getMostSignificantBits());
            out.writeLong(uid.getLeastSignificantBits());
        }
    }

    /**
     * Reads UUID from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static UUID readUuid(DataInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            return IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));
        }

        return null;
    }

    /**
     * Writes {@link org.apache.ignite.lang.IgniteUuid} to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeIgniteUuid(DataOutput out, IgniteUuid uid) throws IOException {
        // Write null flag.
        out.writeBoolean(uid == null);

        if (uid != null) {
            out.writeLong(uid.globalId().getMostSignificantBits());
            out.writeLong(uid.globalId().getLeastSignificantBits());

            out.writeLong(uid.localId());
        }
    }

    /**
     * Reads {@link org.apache.ignite.lang.IgniteUuid} from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static IgniteUuid readIgniteUuid(DataInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            UUID globalId = IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));

            long locId = in.readLong();

            return new IgniteUuid(globalId, locId);
        }

        return null;
    }

    /**
     * Writes int array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws IOException If write failed.
     */
    public static void writeIntArray(DataOutput out, @Nullable int[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (int b : arr)
                out.writeInt(b);
        }
    }

    /**
     * Writes long array to output stream.
     *
     * @param out Output stream to write to.
     * @param arr Array to write.
     * @throws IOException If write failed.
     */
    public static void writeLongArray(DataOutput out, @Nullable long[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (long b : arr)
                out.writeLong(b);
        }
    }

    /**
     * Reads int array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static int[] readIntArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        int[] res = new int[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readInt();

        return res;
    }

    /**
     * Reads long array from input stream.
     *
     * @param in Stream to read from.
     * @return Read long array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static long[] readLongArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        long[] res = new long[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readLong();

        return res;
    }

    /**
     * @param out Output.
     * @param map Map to write.
     * @throws IOException If write failed.
     */
    public static void writeMap(ObjectOutput out, Map<?, ?> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> Map<K, V> readMap(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        Map<K, V> map = new HashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * Calculate a hashCode for an array.
     *
     * @param obj Object.
     */
    public static int hashCode(Object obj) {
        if (obj == null)
            return 0;

        if (obj.getClass().isArray()) {
            if (obj instanceof byte[])
                return Arrays.hashCode((byte[])obj);
            if (obj instanceof short[])
                return Arrays.hashCode((short[])obj);
            if (obj instanceof int[])
                return Arrays.hashCode((int[])obj);
            if (obj instanceof long[])
                return Arrays.hashCode((long[])obj);
            if (obj instanceof float[])
                return Arrays.hashCode((float[])obj);
            if (obj instanceof double[])
                return Arrays.hashCode((double[])obj);
            if (obj instanceof char[])
                return Arrays.hashCode((char[])obj);
            if (obj instanceof boolean[])
                return Arrays.hashCode((boolean[])obj);

            int result = 1;

            for (Object element : (Object[])obj)
                result = 31 * result + hashCode(element);

            return result;
        }
        else
            return obj.hashCode();
    }

    /**
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> TreeMap<K, V> readTreeMap(
        ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        TreeMap<K, V> map = new TreeMap<>();

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * Read hash map.
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> HashMap<K, V> readHashMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        HashMap<K, V> map = newHashMap(size);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> LinkedHashMap<K, V> readLinkedMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        LinkedHashMap<K, V> map = new LinkedHashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * @param in Input.
     * @return Deserialized list.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> List<E> readList(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<E> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add((E)in.readObject());

        return col;
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public static <E> Set<E> readSet(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Set<E> set = new HashSet(size, 1.0f);

        for (int i = 0; i < size; i++)
            set.add((E)in.readObject());

        return set;
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> LinkedHashSet<E> readLinkedSet(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        LinkedHashSet<E> set = new LinkedHashSet<>(size);

        for (int i = 0; i < size; i++)
            set.add((E)in.readObject());

        return set;
    }

    /**
     * Writes string to output stream accounting for {@code null} values.
     * <p>
     * Limitation for max string lenght of <code>65535</code> bytes is caused by {@link DataOutput#writeUTF}
     * used under the hood to perform an actual write.
     * </p>
     * <p>
     * If longer string is passes a {@link UTFDataFormatException} exception will be thrown.
     * </p>
     * <p>
     * To write longer strings use {@link #writeLongString(DataOutput, String)} writes string as is converting it into binary array of UTF-8
     * encoded characters.
     * To read the value back {@link #readLongString(DataInput)} should be used.
     * </p>
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeString(DataOutput out, String s) throws IOException {
        // Write null flag.
        out.writeBoolean(s == null);

        if (s != null)
            out.writeUTF(s);
    }

    /**
     * Reads string from input stream accounting for {@code null} values.
     *
     * Method enables to read strings shorter than <code>65535</code> bytes in UTF-8 otherwise an exception will be thrown.
     *
     * Strings written by {@link #writeString(DataOutput, String)} can be read by this method.
     *
     * @see #writeString(DataOutput, String) for more information about writing strings.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readString(DataInput in) throws IOException {
        // If value is not null, then read it. Otherwise return null.
        return !in.readBoolean() ? in.readUTF() : null;
    }

    /**
     * Writes enum to output stream accounting for {@code null} values.
     * Note: method writes only one byte for every enum. Therefore, this method
     * only for Enums with maximum count of values equals to 128.
     *
     * @param out Output stream to write to.
     * @param e Enum value to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static <E extends Enum<E>> void writeEnum(DataOutput out, E e) throws IOException {
        out.writeByte(e == null ? -1 : e.ordinal());
    }

    /** */
    public static <E extends Enum<E>> E readEnum(DataInput in, Class<E> enumCls) throws IOException {
        byte ordinal = in.readByte();

        if (ordinal == (byte)-1)
            return null;

        int idx = ordinal & 0xFF;

        E[] values = enumCls.getEnumConstants();

        return idx < values.length ? values[idx] : null;
    }

    /**
     * Gets annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        T ann = cls.getAnnotation(annCls);

        if (ann != null)
            return ann;

        for (Class<?> itf : cls.getInterfaces()) {
            ann = getAnnotation(itf, annCls); // Recursion.

            if (ann != null)
                return ann;
        }

        if (!cls.isInterface()) {
            ann = getAnnotation(cls.getSuperclass(), annCls);

            if (ann != null)
                return ann;
        }

        return null;
    }

    /**
     * Gets declared annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        return cls.getDeclaredAnnotation(annCls);
    }

    /**
     * Indicates if class has given declared annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        return getDeclaredAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Object o, Class<T> annCls) {
        return o != null && hasDeclaredAnnotation(o.getClass(), annCls);
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Class<?> cls, Class<T> annCls) {
        return getAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Object o, Class<T> annCls) {
        return o != null && hasAnnotation(o.getClass(), annCls);
    }

    /**
     * Provides all interfaces of {@code cls} including inherited ones. Excludes duplicated ones in case of multiple
     * inheritance.
     *
     * @param cls Class to search for interfaces.
     * @return Collection of interfaces of {@code cls}.
     */
    public static Collection<Class<?>> allInterfaces(Class<?> cls) {
        Set<Class<?>> interfaces = new HashSet<>();

        while (cls != null) {
            interfaces.addAll(Arrays.asList(cls.getInterfaces()));

            cls = cls.getSuperclass();
        }

        return interfaces;
    }

    /**
     * Gets simple class name taking care of empty names.
     *
     * @param cls Class to get the name for.
     * @return Simple class name.
     */
    public static String getSimpleName(Class<?> cls) {
        String name = cls.getSimpleName();

        if (F.isEmpty(name))
            name = cls.getName().substring(cls.getPackage().getName().length() + 1);

        return name;
    }

    /**
     * Checks if the map passed in is contained in base map.
     *
     * @param base Base map.
     * @param map Map to check.
     * @return {@code True} if all entries within map are contained in base map,
     *      {@code false} otherwise.
     */
    public static boolean containsAll(Map<?, ?> base, Map<?, ?> map) {
        assert base != null;
        assert map != null;

        for (Map.Entry<?, ?> entry : map.entrySet())
            if (base.containsKey(entry.getKey())) {
                Object val = base.get(entry.getKey());

                if (val == null && entry.getValue() == null)
                    continue;

                if (val == null || entry.getValue() == null || !val.equals(entry.getValue()))
                    // Mismatch found.
                    return false;
            }
            else
                return false;

        // All entries in 'map' are contained in base map.
        return true;
    }

    /**
     * Gets task name for the given task class.
     *
     * @param taskCls Task class.
     * @return Either task name from class annotation (see {@link org.apache.ignite.compute.ComputeTaskName}})
     *      or task class name if there is no annotation.
     */
    public static String getTaskName(Class<? extends ComputeTask<?, ?>> taskCls) {
        ComputeTaskName nameAnn = getAnnotation(taskCls, ComputeTaskName.class);

        return nameAnn == null ? taskCls.getName() : nameAnn.value();
    }

    /**
     * Gets resource name.
     * Returns a task name if it is a Compute task or a class name otherwise.
     *
     * @param rscCls Class of resource.
     * @return Name of resource.
     */
    public static String getResourceName(Class rscCls) {
        if (ComputeTask.class.isAssignableFrom(rscCls))
            return getTaskName(rscCls);

        return rscCls.getName();
    }

    /**
     * Creates SPI attribute name by adding prefix to the attribute name.
     * Prefix is an SPI name + '.'.
     *
     * @param spi SPI.
     * @param attrName attribute name.
     * @return SPI attribute name.
     */
    public static String spiAttribute(IgniteSpi spi, String attrName) {
        assert spi != null;
        assert spi.getName() != null;

        return spi.getName() + '.' + attrName;
    }

    /**
     * Gets resource path for the class.
     *
     * @param clsName Class name.
     * @return Resource name for the class.
     */
    public static String classNameToResourceName(String clsName) {
        return clsName.replaceAll("\\.", "/") + ".class";
    }

    /**
     * Gets threading MBean.
     *
     * @return Threading MBean.
     */
    public static ThreadMXBean getThreadMx() {
        return ManagementFactory.getThreadMXBean();
    }

    /**
     * Gets amount of RAM memory available on this machine.
     *
     * @return Total amount of memory in bytes or -1 if any exception happened.
     */
    public static long getTotalMemoryAvailable() {
        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        Object attr;

        try {
            attr = mBeanSrv.getAttribute(
                    ObjectName.getInstance("java.lang", "type", "OperatingSystem"),
                    "TotalPhysicalMemorySize");
        }
        catch (Exception e) {
            return -1;
        }

        return (attr instanceof Long) ? (Long)attr : -1;
    }

    /**
     * Gets compilation MBean.
     *
     * @return Compilation MBean.
     */
    public static CompilationMXBean getCompilerMx() {
        return ManagementFactory.getCompilationMXBean();
    }

    /**
     * Tries to detect user class from passed in object inspecting
     * collections, arrays or maps.
     *
     * @param obj Object.
     * @return First non-JDK or deployment aware class or passed in object class.
     */
    public static Class<?> detectClass(Object obj) {
        assert obj != null;

        if (obj instanceof GridPeerDeployAware)
            return ((GridPeerDeployAware)obj).deployClass();

        if (isPrimitiveArray(obj))
            return obj.getClass();

        if (!isJdk(obj.getClass()))
            return obj.getClass();

        if (obj instanceof Iterable<?>) {
            Object o = F.first((Iterable<?>)obj);

            // No point to continue, if null.
            return o != null ? o.getClass() : obj.getClass();
        }

        if (obj instanceof Map) {
            Map.Entry<?, ?> e = F.firstEntry((Map<?, ?>)obj);

            if (e != null) {
                Object k = e.getKey();

                if (k != null && !isJdk(k.getClass()))
                    return k.getClass();

                Object v = e.getValue();

                return v != null ? v.getClass() : obj.getClass();
            }
        }

        if (obj.getClass().isArray()) {
            int len = Array.getLength(obj);

            if (len > 0) {
                Object o = Array.get(obj, 0);

                return o != null ? o.getClass() : obj.getClass();
            }
            else
                return obj.getClass().getComponentType();
        }

        return obj.getClass();
    }

    /**
     * Detects class loader for given class.
     * <p>
     * This method will first check if {@link Thread#getContextClassLoader()} is appropriate.
     * If yes, then context class loader will be returned, otherwise
     * the {@link Class#getClassLoader()} will be returned.
     *
     * @param cls Class to find class loader for.
     * @return Class loader for given class (never {@code null}).
     */
    public static ClassLoader detectClassLoader(Class<?> cls) {
        return GridClassLoaderCache.classLoader(cls);
    }

    /**
     * Detects class loader for given object's class.
     *
     * @param obj Object to find class loader for class of.
     * @return Class loader for given object (possibly {@code null}).
     */
    @Nullable public static ClassLoader detectObjectClassLoader(@Nullable Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof GridPeerDeployAware)
            return ((GridPeerDeployAware)obj).classLoader();

        return detectClassLoader(obj.getClass());
    }

    /**
     * Tests whether or not given class is loadable provided class loader.
     *
     * @param clsName Class name to test.
     * @param ldr Class loader to test with. If {@code null} - we'll use system class loader instead.
     *      If System class loader is not set - this method will return {@code false}.
     * @return {@code True} if class is loadable, {@code false} otherwise.
     */
    public static boolean isLoadableBy(String clsName, @Nullable ClassLoader ldr) {
        assert clsName != null;

        if (ldr == null)
            ldr = gridClassLoader;

        String lambdaParent = lambdaEnclosingClassName(clsName);

        try {
            ldr.loadClass(lambdaParent == null ? clsName : lambdaParent);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Gets the peer deploy aware instance for the object with the widest class loader.
     * If collection is {@code null}, empty or contains only {@code null}s - the peer
     * deploy aware object based on system class loader will be returned.
     *
     * @param c Collection.
     * @return Peer deploy aware object from this collection with the widest class loader.
     * @throws IllegalArgumentException Thrown in case when common class loader for all
     *      elements in this collection cannot be found. In such case - peer deployment
     *      is not possible.
     */
    public static GridPeerDeployAware peerDeployAware0(@Nullable Iterable<?> c) {
        if (!F.isEmpty(c)) {
            assert c != null;

            // We need to find common classloader for all elements AND the collection itself
            Collection<Object> tmpC = new ArrayList<>();

            for (Object e : c)
                tmpC.add(e);

            tmpC.add(c);

            boolean notAllNulls = false;

            for (Object obj : tmpC) {
                if (obj != null) {
                    notAllNulls = true;

                    if (hasCommonClassLoader(obj, tmpC))
                        return obj == c ? peerDeployAware(obj) : peerDeployAware0(obj);
                }
            }

            // If all are nulls - don't throw an exception.
            if (notAllNulls)
                throw new IllegalArgumentException("Failed to find common class loader for all elements in " +
                    "given collection. Peer deployment cannot be performed for such collection.");
        }

        return peerDeployAware(c);
    }

    /**
     * Check if all elements from the collection could be loaded with the same classloader as the given object.
     *
     * @param obj base object.
     * @param c collection to check elements from.
     * @return {@code true} if all elements could be loaded with {@code obj}'s classloader, {@code false} otherwise
     */
    private static boolean hasCommonClassLoader(Object obj, Iterable<?> c) {
        assert obj != null;
        assert c != null;

        ClassLoader ldr = obj instanceof GridPeerDeployAware ?
            ((GridPeerDeployAware)obj).classLoader() : detectClassLoader(obj.getClass());

        boolean found = true;

        for (Object obj2 : c) {
            if (obj2 == null || obj2 == obj)
                continue;

            // Obj2 class name.
            String clsName = obj2 instanceof GridPeerDeployAware ?
                ((GridPeerDeployAware)obj2).deployClass().getName() : obj2.getClass().getName();

            if (!isLoadableBy(clsName, ldr)) {
                found = false;

                break;
            }
        }

        return found;
    }

    /**
     * Gets the peer deploy aware instance for the object with the widest class loader.
     * If array is {@code null}, empty or contains only {@code null}s - the peer
     * deploy aware object based on system class loader will be returned.
     *
     * @param c Objects.
     * @return Peer deploy aware object from this array with the widest class loader.
     * @throws IllegalArgumentException Thrown in case when common class loader for all
     *      elements in this array cannot be found. In such case - peer deployment
     *      is not possible.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    public static GridPeerDeployAware peerDeployAware0(@Nullable Object... c) {
        if (!F.isEmpty(c)) {
            assert c != null;

            boolean notAllNulls = false;

            for (Object obj : c) {
                if (obj != null) {
                    notAllNulls = true;

                    ClassLoader ldr = obj instanceof GridPeerDeployAware ?
                        ((GridPeerDeployAware)obj).classLoader() : obj.getClass().getClassLoader();

                    boolean found = true;

                    for (Object obj2 : c) {
                        if (obj2 == null || obj2 == obj)
                            continue;

                        // Obj2 class name.
                        String clsName = obj2 instanceof GridPeerDeployAware ?
                            ((GridPeerDeployAware)obj2).deployClass().getName() : obj2.getClass().getName();

                        if (!isLoadableBy(clsName, ldr)) {
                            found = false;

                            break;
                        }
                    }

                    if (found)
                        return peerDeployAware0(obj);
                }
            }

            // If all are nulls - don't throw an exception.
            if (notAllNulls)
                throw new IllegalArgumentException("Failed to find common class loader for all elements in " +
                    "given collection. Peer deployment cannot be performed for such collection.");
        }

        return peerDeployAware(new Object[0]);
    }

    /**
     * Creates an instance of {@link GridPeerDeployAware} for object.
     *
     * Checks, if the object is an instance of collection or object
     * array.
     *
     * @param obj Object to deploy.
     * @return {@link GridPeerDeployAware} instance for given object.
     */
    public static GridPeerDeployAware peerDeployAware0(Object obj) {
        if (obj instanceof Iterable)
            return peerDeployAware0((Iterable)obj);

        if (obj.getClass().isArray() && !isPrimitiveArray(obj))
            return peerDeployAware0((Object[])obj);

        return peerDeployAware(obj);
    }

    /**
     * Creates an instance of {@link GridPeerDeployAware} for object.
     *
     * @param obj Object to deploy.
     * @return {@link GridPeerDeployAware} instance for given object.
     */
    public static GridPeerDeployAware peerDeployAware(Object obj) {
        assert obj != null;

        if (obj instanceof GridPeerDeployAware)
            return (GridPeerDeployAware)obj;

        final Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

        return new GridPeerDeployAware() {
            /** */
            private ClassLoader ldr;

            @Override public Class<?> deployClass() {
                return cls;
            }

            @Override public ClassLoader classLoader() {
                if (ldr == null)
                    ldr = detectClassLoader(cls);

                return ldr;
            }
        };
    }

    /**
     * Unwraps top level user class for wrapped objects.
     *
     * @param obj Object to check.
     * @return Top level user class.
     */
    public static GridPeerDeployAware detectPeerDeployAware(GridPeerDeployAware obj) {
        GridPeerDeployAware p = nestedPeerDeployAware(obj, true, new GridLeanIdentitySet<>());

        // Pass in obj.getClass() to avoid infinite recursion.
        return p != null ? p : peerDeployAware(obj.getClass());
    }

    /**
     * Gets peer deploy class if there is any {@link GridPeerDeployAware} within reach.
     *
     * @param obj Object to check.
     * @param top Indicates whether object is top level or a nested field.
     * @param processed Set of processed objects to avoid infinite recursion.
     * @return Peer deploy class, or {@code null} if one could not be found.
     */
    @Nullable private static GridPeerDeployAware nestedPeerDeployAware(Object obj, boolean top, Set<Object> processed) {
        // Avoid infinite recursion.
        if (!processed.add(obj))
            return null;

        if (obj instanceof GridPeerDeployAware) {
            GridPeerDeployAware p = (GridPeerDeployAware)obj;

            if (!top && p.deployClass() != null)
                return p;

            for (Class<?> cls = obj.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass()) {
                // Cache by class name instead of class to avoid infinite growth of the
                // caching map in case of multiple redeployment of the same class.
                IgniteBiTuple<Class<?>, Collection<Field>> tup = p2pFields.get(cls.getName());

                boolean cached = tup != null && tup.get1().equals(cls);

                Iterable<Field> fields = cached ? tup.get2() : Arrays.asList(cls.getDeclaredFields());

                if (!cached) {
                    tup = new IgniteBiTuple<>();

                    tup.set1(cls);
                }

                for (Field f : fields)
                    // Special handling for anonymous classes.
                    if (cached || f.getName().startsWith("this$") || f.getName().startsWith("val$")) {
                        if (!cached) {
                            f.setAccessible(true);

                            if (tup.get2() == null)
                                tup.set2(new LinkedList<Field>());

                            tup.get2().add(f);
                        }

                        try {
                            Object o = f.get(obj);

                            if (o != null) {
                                // Recursion.
                                p = nestedPeerDeployAware(o, false, processed);

                                if (p != null) {
                                    if (!cached)
                                        // Potentially replace identical value
                                        // stored by another thread.
                                        p2pFields.put(cls.getName(), tup);

                                    return p;
                                }
                            }
                        }
                        catch (IllegalAccessException ignored) {
                            return null;
                        }
                    }
            }
        }
        // Don't go into internal Ignite structures.
        else if (isIgnite(obj.getClass()))
            return null;
        else if (obj instanceof Iterable)
            for (Object o : (Iterable<?>)obj) {
                // Recursion.
                GridPeerDeployAware p = nestedPeerDeployAware(o, false, processed);

                if (p != null)
                    return p;
            }
        else if (obj.getClass().isArray()) {
            Class<?> type = obj.getClass().getComponentType();

            // We don't care about primitives or internal JDK types.
            if (!type.isPrimitive() && !isJdk(type)) {
                Object[] arr = (Object[])obj;

                for (Object o : arr) {
                    // Recursion.
                    GridPeerDeployAware p = nestedPeerDeployAware(o, false, processed);

                    if (p != null)
                        return p;
                }
            }
        }

        return null;
    }

    /**
     * Checks if given class is of {@code Ignite} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Ignite} type.
     */
    public static boolean isIgnite(Class<?> cls) {
        String name = cls.getName();

        return name.startsWith("org.apache.ignite") || name.startsWith("org.jsr166");
    }

    /**
     * Checks if given class is of {@code Grid} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Grid} type.
     */
    public static boolean isGrid(Class<?> cls) {
        return cls.getName().startsWith("org.apache.ignite.internal");
    }

    /**
     * Replaces all occurrences of {@code org.apache.ignite.} with {@code o.a.i.},
     * {@code org.apache.ignite.internal.} with {@code o.a.i.i.},
     * {@code org.apache.ignite.internal.visor.} with {@code o.a.i.i.v.} and
     *
     * @param s String to replace in.
     * @return Replaces string.
     */
    public static String compact(String s) {
        return s.replace("org.apache.ignite.internal.visor.", "o.a.i.i.v.").
            replace("org.apache.ignite.internal.", "o.a.i.i.").
            replace(IGNITE_PKG, "o.a.i.");
    }

    /**
     * Check if given class is of JDK type.
     *
     * @param cls Class to check.
     * @return {@code True} if object is JDK type.
     */
    public static boolean isJdk(Class<?> cls) {
        if (cls.isPrimitive())
            return true;

        String s = cls.getName();

        return s.startsWith("java.") || s.startsWith("javax.");
    }

    /**
     * Check if given class represents a Enum.
     *
     * @param cls Class to check.
     * @return {@code True} if this is a Enum class.
     */
    public static boolean isEnum(Class cls) {
        if (cls.isEnum())
            return true;

        Class sCls = cls.getSuperclass();

        return sCls != null && sCls.isEnum();
    }

    /**
     * Converts {@link InterruptedException} to {@link IgniteCheckedException}.
     *
     * @param mux Mux to wait on.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    @SuppressWarnings({"WaitNotInLoop"})
    public static void wait(Object mux) throws IgniteInterruptedCheckedException {
        try {
            mux.wait();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Unzip file to folder.
     *
     * @param zipFile ZIP file.
     * @param toDir Directory to unzip file content.
     * @param log Grid logger.
     * @throws IOException In case of error.
     */
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public static void unzip(File zipFile, File toDir, @Nullable IgniteLogger log) throws IOException {
        ZipFile zip = null;

        try {
            zip = new ZipFile(zipFile);

            for (ZipEntry entry : asIterable(zip.entries())) {
                if (entry.isDirectory()) {
                    // Assume directories are stored parents first then children.
                    new File(toDir, entry.getName()).mkdirs();

                    continue;
                }

                InputStream in = null;
                OutputStream out = null;

                try {
                    in = zip.getInputStream(entry);

                    File outFile = new File(toDir, entry.getName());

                    if (!outFile.getParentFile().exists())
                        outFile.getParentFile().mkdirs();

                    out = new BufferedOutputStream(new FileOutputStream(outFile));

                    copy(in, out);
                }
                finally {
                    close(in, log);
                    close(out, log);
                }
            }
        }
        finally {
            if (zip != null)
                zip.close();
        }
    }

    /**
     * @return {@code True} if assertions enabled.
     */
    public static boolean assertionsEnabled() {
        return assertionsEnabled;
    }

    /**
     * Gets OS string.
     *
     * @return OS string.
     */
    public static String osString() {
        return osStr;
    }

    /**
     * Gets JDK string.
     *
     * @return JDK string.
     */
    public static String jdkString() {
        return jdkStr;
    }

    /**
     * Indicates whether current OS is Linux flavor.
     *
     * @return {@code true} if current OS is Linux - {@code false} otherwise.
     */
    public static boolean isLinux() {
        return linux;
    }

    /**
     * Indicates whether current OS is Mac OS.
     *
     * @return {@code true} if current OS is Mac OS - {@code false} otherwise.
     */
    public static boolean isMacOs() {
        return mac;
    }

    /**
     * Indicates whether current OS is UNIX flavor.
     *
     * @return {@code true} if current OS is UNIX - {@code false} otherwise.
     */
    public static boolean isUnix() {
        return unix;
    }

    /**
     * Indicates whether current OS is Windows.
     *
     * @return {@code true} if current OS is Windows (any versions) - {@code false} otherwise.
     */
    public static boolean isWindows() {
        return win;
    }

    /**
     * Gets JVM implementation name.
     *
     * @return JVM implementation name.
     */
    public static String jvmName() {
        return jvmImplName;
    }

    /**
     * Does a best effort to detect if we a running on a 32-bit JVM.
     *
     * @return {@code true} if detected that we are running on a 32-bit JVM.
     */
    public static boolean jvm32Bit() {
        return jvm32Bit;
    }

    /**
     * Gets node product version based on node attributes.
     *
     * @param node Node to get version from.
     * @return Version object.
     */
    public static IgniteProductVersion productVersion(ClusterNode node) {
        String verStr = node.attribute(ATTR_BUILD_VER);
        String buildDate = node.attribute(ATTR_BUILD_DATE);

        if (buildDate != null)
            verStr += '-' + buildDate;

        return IgniteProductVersion.fromString(verStr);
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Callable to run.
     * @param <R> Return type.
     * @return Return value.
     * @throws IgniteCheckedException If call failed.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, Callable<R> c) throws IgniteCheckedException {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.call();
        }
        catch (IgniteCheckedException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     * @param <R> Return type.
     * @return Return value.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, IgniteOutClosure<R> c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.apply();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     */
    public static void wrapThreadLoader(ClassLoader ldr, Runnable c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            c.run();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Short node representation.
     *
     * @param n Grid node.
     * @return Short string representing the node.
     */
    public static String toShortString(ClusterNode n) {
        return "ClusterNode [id=" + n.id() + ", order=" + n.order() + ", addr=" + n.addresses() + ']';
    }

    /**
     * Short node representation.
     *
     * @param ns Grid nodes.
     * @return Short string representing the node.
     */
    public static String toShortString(Collection<? extends ClusterNode> ns) {
        SB sb = new SB("Grid nodes [cnt=" + ns.size());

        for (ClusterNode n : ns)
            sb.a(", ").a(toShortString(n));

        return sb.a(']').toString();
    }

    /**
     * Get string representation of an object properly catching all exceptions.
     *
     * @param obj Object.
     * @return Result or {@code null}.
     */
    @Nullable public static String toStringSafe(@Nullable Object obj) {
        if (obj == null)
            return null;
        else {
            try {
                return obj.toString();
            }
            catch (Exception e) {
                try {
                    return "Failed to convert object to string: " + e.getMessage();
                }
                catch (Exception e0) {
                    return "Failed to convert object to string (error message is not available)";
                }
            }
        }
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static int[] toIntArray(@Nullable Collection<Integer> c) {
        if (c == null || c.isEmpty())
            return EMPTY_INTS;

        int[] arr = new int[c.size()];

        int idx = 0;

        for (Integer i : c)
            arr[idx++] = i;

        return arr;
    }

    /**
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    public static int[] addAll(int[] arr1, int[] arr2) {
        int[] all = new int[arr1.length + arr2.length];

        System.arraycopy(arr1, 0, all, 0, arr1.length);
        System.arraycopy(arr2, 0, all, arr1.length, arr2.length);

        return all;
    }

    /**
     * Converts array of integers into list.
     *
     * @param arr Array of integers.
     * @param p Optional predicate array.
     * @return List of integers.
     */
    public static List<Integer> toIntList(@Nullable int[] arr, IgnitePredicate<Integer>... p) {
        if (arr == null || arr.length == 0)
            return Collections.emptyList();

        List<Integer> ret = new ArrayList<>(arr.length);

        if (F.isEmpty(p))
            for (int i : arr)
                ret.add(i);
        else {
            for (int i : arr)
                if (F.isAll(i, p))
                    ret.add(i);
        }

        return ret;
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static long[] toLongArray(@Nullable Collection<Long> c) {
        if (c == null || c.isEmpty())
            return EMPTY_LONGS;

        long[] arr = new long[c.size()];

        int idx = 0;

        for (Long l : c)
            arr[idx++] = l;

        return arr;
    }

    /**
     * Concats two integers to long.
     *
     * @param high Highest bits.
     * @param low Lowest bits.
     * @return Long.
     */
    public static long toLong(int high, int low) {
        return (((long)high) << Integer.SIZE) | (low & 0xffffffffL);
    }

    /**
     * Copies all elements from collection to array and asserts that
     * array is big enough to hold the collection. This method should
     * always be preferred to {@link Collection#toArray(Object[])}
     * method.
     *
     * @param c Collection to convert to array.
     * @param arr Array to populate.
     * @param <T> Element type.
     * @return Passed in array.
     */
    public static <T> T[] toArray(Collection<? extends T> c, T[] arr) {
        T[] a = c.toArray(arr);

        assert a == arr;

        return arr;
    }

    /**
     * Returns array which is the union of two arrays
     * (array of elements contained in any of provided arrays).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is union of {@code a} and {@code b}.
     */
    public static int[] unique(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen + bLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                res[resLen++] = b[j++];
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        while (j < bLen)
            res[resLen++] = b[j++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Returns array which is the difference between two arrays
     * (array of elements contained in first array but not contained in second).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is difference between {@code a} and {@code b}.
     */
    public static int[] difference(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                j++;
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Checks if array prefix increases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) increases.
     */
    public static boolean isIncreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] >= arr[i])
                return false;
        }

        return true;
    }

    /**
     * Checks if array prefix do not decreases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) do not decreases.
     */
    public static boolean isNonDecreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] > arr[i])
                return false;
        }

        return true;
    }

    /**
     * Copies array only if array length greater than needed length.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return Old array if length of {@code arr} is equals to {@code len},
     *      otherwise copy of array.
     */
    public static int[] copyIfExceeded(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        return len == arr.length ? arr : Arrays.copyOf(arr, len);
    }

    /**
     * Utility method creating {@link JMException} with given cause. Keeps only the error message to avoid
     * deserialization failure on remote side due to the other class path.
     *
     * @param e Cause exception.
     * @return Newly created {@link JMException}.
     */
    public static JMException jmException(Throwable e) {
        return new JMException(e.getMessage());
    }

    /**
     * Unwraps closure exceptions.
     *
     * @param t Exception.
     * @return Unwrapped exception.
     */
    public static Exception unwrap(Throwable t) {
        assert t != null;

        while (true) {
            if (t instanceof Error)
                throw (Error)t;

            if (t instanceof GridClosureException) {
                t = ((GridClosureException)t).unwrap();

                continue;
            }

            return (Exception)t;
        }
    }

    /**
     * Casts the passed {@code Throwable t} to {@link IgniteCheckedException}.<br>
     * If {@code t} is a {@link GridClosureException}, it is unwrapped and then cast to {@link IgniteCheckedException}.
     * If {@code t} is an {@link IgniteCheckedException}, it is returned.
     * If {@code t} is not a {@link IgniteCheckedException}, a new {@link IgniteCheckedException} caused by {@code t}
     * is returned.
     *
     * @param t Throwable to cast.
     * @return {@code t} cast to {@link IgniteCheckedException}.
     */
    public static IgniteCheckedException cast(Throwable t) {
        assert t != null;

        t = unwrap(t);

        return t instanceof IgniteCheckedException
            ? (IgniteCheckedException)t
            : new IgniteCheckedException(t);
    }

    /**
     * Checks if class loader is an internal P2P class loader.
     *
     * @param ldr Class loader to check.
     * @return {@code True} if P2P class loader.
     */
    public static boolean p2pLoader(ClassLoader ldr) {
        return ldr instanceof GridDeploymentInfo;
    }

    /**
     * Returns Deployment class loader id if method was invoked in the job context
     * (it may be the context of a cache's operation which was triggered by the distributed job)
     * or {@code null} if no context was found or Deployment is switched off.
     *
     * @param ctx Kernal context.
     * @return Deployment class loader id or {@code null}.
     */
    public static IgniteUuid contextDeploymentClassLoaderId(GridKernalContext ctx) {
        if (ctx == null || !ctx.deploy().enabled())
            return null;

        if (ctx.job() != null && ctx.job().currentDeployment() != null)
            return ctx.job().currentDeployment().classLoaderId();

        if (ctx.cache() != null && ctx.cache().context() != null)
            return ctx.cache().context().deploy().locLoaderId();

        return null;
    }

    /**
     * Gets that deployment class loader matching by the specific id, or {@code null}
     * if the class loader was not found.
     *
     * @param ctx Kernal context.
     * @param ldrId Class loader id.
     * @return Deployment class loader or {@code null}.
     */
    public static ClassLoader deploymentClassLoader(GridKernalContext ctx, IgniteUuid ldrId) {
        if (ldrId == null || !ctx.deploy().enabled())
            return null;

        GridDeployment dep = ctx.deploy().getDeployment(ldrId);

        return dep == null ? null : dep.classLoader();
    }

    /**
     * Restores a deployment context for cache deployment.
     *
     * @param ctx Kernal context.
     * @param ldrId Class loader id.
     */
    public static void restoreDeploymentContext(GridKernalContext ctx, IgniteUuid ldrId) {
        if (ctx.deploy().enabled() && ldrId != null) {
            GridDeployment dep = ctx.deploy().getDeployment(ldrId);

            if (dep != null) {
                try {
                    ctx.cache().context().deploy().p2pContext(
                        dep.classLoaderId().globalId(),
                        dep.classLoaderId(),
                        dep.userVersion(),
                        dep.deployMode(),
                        dep.participants()
                    );
                }
                catch (IgnitePeerToPeerClassLoadingException e) {
                    ctx.log(ctx.cache().context().deploy().getClass())
                        .error("Could not restore P2P context [ldrId=" + ldrId + ']', e);
                }
            }
        }
    }

    /**
     * @param ctx Kernal context.
     * @return Closure that converts node ID to a node.
     */
    public static IgniteClosure<UUID, ClusterNode> id2Node(final GridKernalContext ctx) {
        assert ctx != null;

        return new C1<UUID, ClusterNode>() {
            @Nullable @Override public ClusterNode apply(UUID id) {
                return ctx.discovery().node(id);
            }
        };
    }

    /**
     * Checks if object is a primitive array.
     *
     * @param obj Object to check.
     * @return {@code True} if Object is primitive array.
     */
    public static boolean isPrimitiveArray(Object obj) {
        if (obj == null)
            return false;

        Class<?> cls = obj.getClass();

        return cls.isArray() && cls.getComponentType().isPrimitive();
    }

    /**
     * Awaits for condition ignoring interrupts.
     *
     * @param cond Condition to await for.
     */
    public static void awaitQuiet(Condition cond) {
        cond.awaitUninterruptibly();
    }

    /**
     * Awaits for condition.
     *
     * @param cond Condition to await for.
     * @param time The maximum time to wait,
     * @param unit The unit of the {@code time} argument.
     * @return {@code false} if the waiting time detectably elapsed before return from the method, else {@code true}
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}
     */
    public static boolean await(Condition cond, long time, TimeUnit unit) throws IgniteInterruptedCheckedException {
        try {
            return cond.await(time, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch.
     *
     * @param latch Latch to wait for.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void await(CountDownLatch latch) throws IgniteInterruptedCheckedException {
        try {
            if (latch.getCount() > 0)
                latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch.
     *
     * @param latch Latch to wait for.
     * @param timeout Maximum time to wait.
     * @param unit Time unit for timeout.
     * @return {@code True} if the count reached zero and {@code false}
     *      if the waiting time elapsed before the count reached zero.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static boolean await(CountDownLatch latch, long timeout, TimeUnit unit)
        throws IgniteInterruptedCheckedException {
        try {
            return latch.await(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch until it is counted down,
     * ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be
     * recovered prior to return.
     *
     * @param latch Latch to wait for.
     */
    public static void awaitQuiet(CountDownLatch latch) {
        boolean interrupted = false;

        while (true) {
            try {
                latch.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Awaits for the barrier ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be recovered prior to return. If the barrier is
     * already broken, return immediately without throwing any exceptions.
     *
     * @param barrier Barrier to wait for.
     */
    public static void awaitQuiet(CyclicBarrier barrier) {
        boolean interrupted = false;

        while (true) {
            try {
                barrier.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
            catch (BrokenBarrierException ignored) {
                break;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Returns URLs of class loader
     *
     * @param clsLdr Class loader.
     */
    public static URL[] classLoaderUrls(ClassLoader clsLdr) {
        if (clsLdr == null)
            return EMPTY_URL_ARR;
        else if (clsLdr instanceof URLClassLoader)
            return ((URLClassLoader)clsLdr).getURLs();
        else if (bltClsLdrCls != null && urlClsLdrField != null && bltClsLdrCls.isAssignableFrom(clsLdr.getClass())) {
            try {
                synchronized (urlClsLdrField) {
                    // Backup accessible field state.
                    boolean accessible = urlClsLdrField.isAccessible();

                    try {
                        if (!accessible)
                            urlClsLdrField.setAccessible(true);

                        Object ucp = urlClsLdrField.get(clsLdr);

                        if (ucp instanceof URLClassLoader)
                            return ((URLClassLoader)ucp).getURLs();
                        else if (clsURLClassPath != null && clsURLClassPath.isInstance(ucp))
                            return (URL[])mthdURLClassPathGetUrls.invoke(ucp);
                        else
                            throw new RuntimeException("Unknown classloader: " + clsLdr.getClass());
                    }
                    finally {
                        // Recover accessible field state.
                        if (!accessible)
                            urlClsLdrField.setAccessible(false);
                    }
                }
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace(System.err);

                return EMPTY_URL_ARR;
            }
        }
        else
            return EMPTY_URL_ARR;
    }

    /** */
    @Nullable private static Class defaultClassLoaderClass() {
        try {
            return Class.forName("jdk.internal.loader.BuiltinClassLoader");
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    /** */
    @Nullable private static Field urlClassLoaderField() {
        try {
            Class cls = defaultClassLoaderClass();

            return cls == null ? null : cls.getDeclaredField("ucp");
        }
        catch (NoSuchFieldException e) {
            return null;
        }
    }

    /**
     * Sleeps for given number of milliseconds.
     *
     * @param ms Time to sleep.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void sleep(long ms) throws IgniteInterruptedCheckedException {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Joins worker.
     *
     * @param w Worker.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void join(GridWorker w) throws IgniteInterruptedCheckedException {
        try {
            if (w != null)
                w.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Gets result from the given future with right exception handling.
     *
     * @param fut Future.
     * @return Future result.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T get(Future<T> fut) throws IgniteCheckedException {
        try {
            return fut.get();
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException(e.getCause());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
        catch (CancellationException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Joins thread.
     *
     * @param t Thread.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void join(@Nullable Thread t) throws IgniteInterruptedCheckedException {
        if (t == null)
            return;

        try {
            t.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Acquires a permit from provided semaphore.
     *
     * @param sem Semaphore.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void acquire(Semaphore sem) throws IgniteInterruptedCheckedException {
        try {
            sem.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Tries to acquire a permit from provided semaphore during {@code timeout}.
     *
     * @param sem Semaphore.
     * @param timeout The maximum time to wait.
     * @param unit The unit of the {@code time} argument.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     * @return {@code True} if acquires a permit, {@code false} another.
     */
    public static boolean tryAcquire(Semaphore sem, long timeout, TimeUnit unit)
        throws IgniteInterruptedCheckedException {
        try {
            return sem.tryAcquire(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Converts collection of Grid instances to collection of grid names.
     *
     * @param grids Grids.
     * @return Grid names.
     */
    public static Collection<String> grids2names(@Nullable Collection<? extends Ignite> grids) {
        return F.viewReadOnly(grids, new C1<Ignite, String>() {
            @Override public String apply(Ignite g) {
                return g.name();
            }
        });
    }

    /**
     * Converts collection of grid nodes to collection of grid names.
     *
     * @param nodes Nodes.
     * @return Grid names.
     */
    public static Collection<String> nodes2names(@Nullable Collection<? extends ClusterNode> nodes) {
        return F.viewReadOnly(nodes, new C1<ClusterNode, String>() {
            @Override public String apply(ClusterNode n) {
                return G.ignite(n.id()).name();
            }
        });
    }

    /**
     * Initializes logger into/from log reference passed in.
     *
     * @param ctx Context.
     * @param logRef Log reference.
     * @param obj Object to get logger for.
     * @return Logger for the object.
     */
    public static IgniteLogger logger(GridKernalContext ctx, AtomicReference<IgniteLogger> logRef, Object obj) {
        IgniteLogger log = logRef.get();

        if (log == null) {
            logRef.compareAndSet(null, ctx.log(obj.getClass()));

            log = logRef.get();
        }

        return log;
    }

    /**
     * Initializes logger into/from log reference passed in.
     *
     * @param ctx Context.
     * @param logRef Log reference.
     * @param cls Class to get logger for.
     * @return Logger for the object.
     */
    public static IgniteLogger logger(GridKernalContext ctx, AtomicReference<IgniteLogger> logRef, Class<?> cls) {
        IgniteLogger log = logRef.get();

        if (log == null) {
            logRef.compareAndSet(null, ctx.log(cls));

            log = logRef.get();
        }

        return log;
    }

    /**
     * Gets field value.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Field value.
     */
    public static <T> T field(Object obj, String fieldName) {
        assert obj != null;
        assert fieldName != null;

        try {
            for (Class cls = obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : cls.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        field.setAccessible(true);

                        return (T)field.get(obj);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']', e);
        }

        throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']');
    }

    /**
     * Check that field exist.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Boolean flag.
     */
    public static boolean hasField(Object obj, String fieldName) {
        try {
            field(obj, fieldName);

            return true;
        }
        catch (IgniteException e) {
            return false;
        }
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if class is final.
     */
    public static boolean isFinal(Class<?> cls) {
        return Modifier.isFinal(cls.getModifiers());
    }

    /**
     * Gets field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T field(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            for (Class c = cls; cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : c.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        if (!Modifier.isStatic(field.getModifiers()))
                            throw new IgniteCheckedException("Failed to get class field (field is not static) [cls=" +
                                cls + ", fieldName=" + fieldName + ']');

                        boolean accessible = field.isAccessible();

                        T val;

                        try {
                            field.setAccessible(true);

                            val = (T)field.get(null);
                        }
                        finally {
                            if (!accessible)
                                field.setAccessible(false);
                        }

                        return val;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to get field value (field was not found) [fieldName=" + fieldName +
            ", cls=" + cls + ']');
    }

    /**
     * Invokes method.
     *
     * @param cls Object.
     * @param obj Object.
     * @param mtdName Field name.
     * @param params Parameters.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T invoke(@Nullable Class<?> cls, @Nullable Object obj, String mtdName,
        Object... params) throws IgniteCheckedException {
        assert cls != null || obj != null;
        assert mtdName != null;

        try {
            for (cls = cls != null ? cls : obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                Method mtd = null;

                for (Method declaredMtd : cls.getDeclaredMethods()) {
                    if (declaredMtd.getName().equals(mtdName)) {
                        if (mtd == null)
                            mtd = declaredMtd;
                        else
                            throw new IgniteCheckedException("Failed to invoke (ambigous method name) [mtdName=" +
                                mtdName + ", cls=" + cls + ']');
                    }
                }

                if (mtd == null)
                    continue;

                boolean accessible = mtd.isAccessible();

                T res;

                try {
                    mtd.setAccessible(true);

                    res = (T)mtd.invoke(obj, params);
                }
                finally {
                    if (!accessible)
                        mtd.setAccessible(false);
                }

                return res;
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke [mtdName=" + mtdName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to invoke (method was not found) [mtdName=" + mtdName +
            ", cls=" + cls + ']');
    }

    /**
     * Gets property value.
     *
     * @param obj Object.
     * @param propName Property name.
     * @return Field value.
     */
    public static <T> T property(Object obj, String propName) {
        assert obj != null;
        assert propName != null;

        try {
            Method m;

            try {
                m = obj.getClass().getMethod("get" + capitalFirst(propName));
            }
            catch (NoSuchMethodException ignored) {
                m = obj.getClass().getMethod("is" + capitalFirst(propName));
            }

            assert F.isEmpty(m.getParameterTypes());

            boolean accessible = m.isAccessible();

            try {
                m.setAccessible(true);

                return (T)m.invoke(obj);
            }
            finally {
                m.setAccessible(accessible);
            }
        }
        catch (Exception e) {
            throw new IgniteException(
                "Failed to get property value [property=" + propName + ", obj=" + obj + ']', e);
        }
    }

    /**
     * Gets static field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T staticField(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            do {
                for (Field field : cls.getDeclaredFields())
                    if (field.getName().equals(fieldName)) {
                        boolean accessible = field.isAccessible();

                        if (!accessible)
                            field.setAccessible(true);

                        T val = (T)field.get(null);

                        if (!accessible)
                            field.setAccessible(false);

                        return val;
                    }

                if (cls == Object.class)
                    break;

                cls = cls.getSuperclass();
            }
            while (true);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']', e);
        }

        throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']');
    }

    /**
     * Capitalizes the first character of the given string.
     *
     * @param str String.
     * @return String with capitalized first character.
     */
    private static String capitalFirst(@Nullable String str) {
        return str == null ? null :
            str.isEmpty() ? "" : Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    /**
     * Checks whether property is one added by Visor when node is started via remote SSH session.
     *
     * @param name Property name to check.
     * @return {@code True} if property is Visor node startup property, {@code false} otherwise.
     */
    public static boolean isVisorNodeStartProperty(String name) {
        return IGNITE_SSH_HOST.equals(name) || IGNITE_SSH_USER_NAME.equals(name);
    }

    /**
     * Checks whether property is one required by Visor to work correctly.
     *
     * @param name Property name to check.
     * @return {@code True} if property is required by Visor, {@code false} otherwise.
     */
    public static boolean isVisorRequiredProperty(String name) {
        return "java.version".equals(name) || "java.vm.name".equals(name) || "os.arch".equals(name) ||
            "os.name".equals(name) || "os.version".equals(name);
    }

    /**
     * Adds no-op console handler for root java logger.
     *
     * @return Removed handlers.
     */
    public static Collection<Handler> addJavaNoOpLogger() {
        Collection<Handler> savedHnds = new ArrayList<>();

        Logger log = Logger.getLogger("");

        for (Handler h : log.getHandlers()) {
            log.removeHandler(h);

            savedHnds.add(h);
        }

        ConsoleHandler hnd = new ConsoleHandler();

        hnd.setLevel(Level.OFF);

        log.addHandler(hnd);

        return savedHnds;
    }

    /**
     * Removes previously added no-op handler for root java logger.
     *
     * @param rmvHnds Previously removed handlers.
     */
    public static void removeJavaNoOpLogger(Collection<Handler> rmvHnds) {
        Logger log = Logger.getLogger("");

        for (Handler h : log.getHandlers())
            log.removeHandler(h);

        if (!F.isEmpty(rmvHnds)) {
            for (Handler h : rmvHnds)
                log.addHandler(h);
        }
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * Gets absolute value for long. If argument is {@link Long#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Argument.
     * @return Absolute value.
     */
    public static long safeAbs(long i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * When {@code long} value given is positive returns that value, otherwise returns provided default value.
     *
     * @param i Input value.
     * @param dflt Default value.
     * @return {@code i} if {@code i > 0} and {@code dflt} otherwise.
     */
    public static long ensurePositive(long i, long dflt) {
        return i <= 0 ? dflt : i;
    }

    /**
     * Gets wrapper class for a primitive type.
     *
     * @param cls Class. If {@code null}, method is no-op.
     * @return Wrapper class or original class if it is non-primitive.
     */
    @Nullable public static Class<?> box(@Nullable Class<?> cls) {
        if (cls == null)
            return null;

        if (!cls.isPrimitive())
            return cls;

        return boxedClsMap.get(cls);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return forName(clsName, ldr, null, GridBinaryMarshaller.USE_CACHE.get());
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter
    ) throws ClassNotFoundException {
        return forName(clsName, ldr, clsFilter, GridBinaryMarshaller.USE_CACHE.get());
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param useCache If true class loader and result should be cached internally, false otherwise.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter,
        boolean useCache
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null)
            return cls;

        if (ldr != null) {
            if (ldr instanceof ClassCache)
                return ((ClassCache)ldr).getFromCache(clsName);
            else if (!useCache) {
                cls = Class.forName(clsName, true, ldr);

                return cls;
            }
        }
        else
            ldr = gridClassLoader;

        if (!useCache) {
            cls = Class.forName(clsName, true, ldr);

            return cls;
        }

        ConcurrentMap<String, Class> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.apply(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            // Avoid class caching inside Class.forName
            if (ldr instanceof CacheClassLoaderMarker)
                cls = ldr.loadClass(clsName);
            else
                cls = Class.forName(clsName, true, ldr);

            Class old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Clears class associated with provided class loader from class cache.
     *
     * @param ldr Class loader.
     * @param clsName Class name of clearing class.
     */
    public static void clearClassFromClassCache(ClassLoader ldr, String clsName) {
        ConcurrentMap<String, Class> map = classCache.get(ldr);

        if (map != null)
            map.remove(clsName);
    }

    /**
     * Clears class cache for provided loader.
     *
     * @param ldr Class loader.
     */
    public static void clearClassCache(ClassLoader ldr) {
        classCache.remove(ldr);
    }

    /**
     * Completely clears class cache.
     */
    public static void clearClassCache() {
        classCache.clear();
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    public static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * A primitive override of {@link #hash(Object)} to avoid unnecessary boxing.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(long key) {
        int val = (int)(key ^ (key >>> 32));

        return hash(val);
    }

    /**
     * @return PID of the current JVM or {@code -1} if it can't be determined.
     */
    public static int jvmPid() {
        // Should be something like this: 1160@mbp.local
        String name = ManagementFactory.getRuntimeMXBean().getName();

        try {
            int idx = name.indexOf('@');

            return idx > 0 ? Integer.parseInt(name.substring(0, idx)) : -1;
        }
        catch (NumberFormatException ignored) {
            return -1;
        }
    }

    /**
     * @return Input arguments passed to the JVM which does not include the arguments to the <tt>main</tt> method.
     */
    public static List<String> jvmArgs() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments();
    }

    /**
     * As long as array copying uses JVM-private API, which is not guaranteed
     * to be available on all JVM, this method should be called to ensure
     * logic could work properly.
     *
     * @return {@code True} if unsafe copying can work on the current JVM or
     *      {@code false} if it can't.
     */
    @SuppressWarnings("TypeParameterExtendsFinalClass")
    private static boolean unsafeByteArrayCopyAvailable() {
        try {
            Class<? extends Unsafe> unsafeCls = Unsafe.class;

            unsafeCls.getMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
    }

    /**
     * @param src Buffer to copy from (length included).
     * @param off Offset in source buffer.
     * @param resBuf Result buffer.
     * @param resOff Result offset.
     * @param len Length.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int arrayCopy(byte[] src, int off, byte[] resBuf, int resOff, int len) {
        assert resBuf.length >= resOff + len;

        if (UNSAFE_BYTE_ARR_CP)
            GridUnsafe.copyMemory(src, GridUnsafe.BYTE_ARR_OFF + off, resBuf, GridUnsafe.BYTE_ARR_OFF + resOff, len);
        else
            System.arraycopy(src, off, resBuf, resOff, len);

        return resOff + len;
    }

    /**
     * @param addrs Node's addresses.
     * @return A string compatible with {@link ClusterNode#consistentId()} requirements.
     */
    public static String consistentId(Collection<String> addrs) {
        assert !F.isEmpty(addrs);

        StringBuilder sb = new StringBuilder();

        for (String addr : addrs)
            sb.append(addr).append(',');

        sb.delete(sb.length() - 1, sb.length());

        return sb.toString();
    }

    /**
     * @param addrs Node's addresses.
     * @param port Port discovery number.
     * @return A string compatible with {@link ClusterNode#consistentId()} requirements.
     */
    public static String consistentId(Collection<String> addrs, int port) {
        assert !F.isEmpty(addrs);

        return consistentId(addrs) + ':' + port;
    }

    /**
     * Masks name for a valid directory path.
     *
     * @param name Name.
     * @return Masked name.
     */
    public static String maskForFileName(CharSequence name) {
        StringBuilder b = new StringBuilder(name.length());

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);

            if (Character.isLetterOrDigit(c))
                b.append(c);
            else
                b.append('_');
        }

        return b.toString();
    }

    /**
     * Checks if error is MAC invalid argument error which ususally requires special handling.
     *
     * @param e Exception.
     * @return {@code True} if error is invalid argument error on MAC.
     */
    public static boolean isMacInvalidArgumentError(Exception e) {
        return isMacOs() && e instanceof SocketException && e.getMessage() != null &&
            e.getMessage().toLowerCase().contains("invalid argument");
    }

    /**
     * Returns a first non-null value in a given array, if such is present.
     *
     * @param vals Input array.
     * @return First non-null value, or {@code null}, if array is empty or contains
     *      only nulls.
     */
    @Nullable public static <T> T firstNotNull(@Nullable T... vals) {
        if (vals == null)
            return null;

        for (T val : vals) {
            if (val != null)
                return val;
        }

        return null;
    }

    /**
     * For each object provided by the given {@link Iterable} checks if it implements
     * {@link LifecycleAware} interface and executes {@link LifecycleAware#start} method.
     *
     * @param objs Objects.
     * @throws IgniteCheckedException If {@link LifecycleAware#start} fails.
     */
    public static void startLifecycleAware(Iterable<?> objs) throws IgniteCheckedException {
        try {
            for (Object obj : objs) {
                if (obj instanceof LifecycleAware)
                    ((LifecycleAware)obj).start();
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to start component: " + e, e);
        }
    }

    /**
     * For each object provided by the given {@link Iterable} checks if it implements {@link org.apache.ignite.lifecycle.LifecycleAware}
     * interface and executes {@link org.apache.ignite.lifecycle.LifecycleAware#stop} method.
     *
     * @param log Logger used to log error message in case of stop failure.
     * @param objs Object passed to Ignite configuration.
     */
    public static void stopLifecycleAware(IgniteLogger log, Iterable<?> objs) {
        for (Object obj : objs) {
            if (obj instanceof LifecycleAware) {
                try {
                    ((LifecycleAware)obj).stop();
                }
                catch (Exception e) {
                    error(log, "Failed to stop component (ignoring): " + obj, e);
                }
            }
        }
    }

    /**
     * Groups given nodes by the node's physical computer (host).
     * <p>
     * Detection of the same physical computer (host) is based on comparing set of network interface MACs.
     * If two nodes have the same set of MACs, Ignite considers these nodes running on the same
     * physical computer.
     *
     * @param nodes Nodes.
     * @return Collection of projections where each projection represents all nodes (in this projection)
     *      from a single physical computer. Result collection can be empty if this projection is empty.
     */
    public static Map<String, Collection<ClusterNode>> neighborhood(Iterable<ClusterNode> nodes) {
        Map<String, Collection<ClusterNode>> map = new HashMap<>();

        for (ClusterNode n : nodes) {
            String macs = n.attribute(ATTR_MACS);

            assert macs != null : "Missing MACs attribute: " + n;

            Collection<ClusterNode> neighbors = map.get(macs);

            if (neighbors == null)
                map.put(macs, neighbors = new ArrayList<>(2));

            neighbors.add(n);
        }

        return map;
    }

    /**
     * Returns the list of resolved socket addresses.
     *
     * @param node Grid node.
     * @param port Port.
     * @return Socket addresses for given addresses and host names.
     */
    public static Collection<InetSocketAddress> toSocketAddresses(ClusterNode node, int port) {
        return toSocketAddresses(node.addresses(), node.hostNames(), port);
    }

    /**
     * Returns the list of resolved socket addresses.
     *
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @param port Port.
     * @return Socket addresses for given addresses and host names.
     */
    public static Collection<InetSocketAddress> toSocketAddresses(
        Collection<String> addrs,
        Collection<String> hostNames,
        int port
    ) {
        Set<InetSocketAddress> res = new HashSet<>(addrs.size());

        boolean hasAddr = false;

        for (String addr : addrs) {
            InetSocketAddress inetSockAddr = createResolved(addr, port);
            res.add(inetSockAddr);

            if (!inetSockAddr.isUnresolved() && !inetSockAddr.getAddress().isLoopbackAddress())
                hasAddr = true;
        }

        // Try to resolve addresses from host names if no external addresses found.
        if (!hasAddr) {
            for (String host : hostNames) {
                InetSocketAddress inetSockAddr = createResolved(host, port);

                if (!inetSockAddr.isUnresolved())
                    res.add(inetSockAddr);
            }
        }

        return res;
    }

    /**
     * Creates a resolved inet socket address, writing the diagnostic information into a log if operation took
     * a significant amount of time.
     *
     * @param addr Host address.
     * @param port Port value.
     * @return Resolved address.
     */
    private static InetSocketAddress createResolved(String addr, int port) {
        log.log(Level.FINE, () -> S.toString(
            "Resolving address",
            "addr", addr, false,
            "port", port, false,
            "thread", Thread.currentThread().getName(), false
        ));

        long startNanos = System.nanoTime();

        try {
            return new InetSocketAddress(addr, port);
        }
        finally {
            long endNanos = System.nanoTime();

            long duration = endNanos - startNanos;

            long threshold = millisToNanos(200);

            if (duration > threshold) {
                log.log(Level.FINE, new TimeoutException(), () -> S.toString(
                    "Resolving address took too much time",
                    "duration(ms)", nanosToMillis(duration), false,
                    "addr", addr, false,
                    "port", port, false,
                    "thread", Thread.currentThread().getName(), false
                ));
            }
        }
    }

    /**
     * Resolves all not loopback addresses and collect results.
     *
     * @param addrRslvr Address resolver.
     * @param addrs Addresses.
     * @param port Port.
     * @return Resolved socket addresses.
     * @throws IgniteSpiException If failed.
     */
    public static Collection<InetSocketAddress> resolveAddresses(
        AddressResolver addrRslvr,
        Iterable<String> addrs,
        int port
    ) throws IgniteSpiException {
        assert addrRslvr != null;

        Collection<InetSocketAddress> extAddrs = new HashSet<>();

        for (String addr : addrs) {
            InetSocketAddress sockAddr = new InetSocketAddress(addr, port);

            if (!sockAddr.isUnresolved()) {
                Collection<InetSocketAddress> extAddrs0 = resolveAddress(addrRslvr, sockAddr);

                if (extAddrs0 != null)
                    extAddrs.addAll(extAddrs0);
            }
        }

        return extAddrs;
    }

    /**
     * @param addrRslvr Address resolver.
     * @param sockAddr Addresses.
     * @return Resolved addresses.
     */
    public static Collection<InetSocketAddress> resolveAddresses(AddressResolver addrRslvr,
        Collection<InetSocketAddress> sockAddr) {
        if (addrRslvr == null)
            return sockAddr;

        Collection<InetSocketAddress> resolved = new HashSet<>();

        for (InetSocketAddress addr : sockAddr)
            resolved.addAll(resolveAddress(addrRslvr, addr));

        return resolved;
    }

    /**
     * @param addrRslvr Address resolver.
     * @param sockAddr Addresses.
     * @return Resolved addresses.
     */
    private static Collection<InetSocketAddress> resolveAddress(AddressResolver addrRslvr, InetSocketAddress sockAddr) {
        try {
            return addrRslvr.getExternalAddresses(sockAddr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to get mapped external addresses " +
                "[addrRslvr=" + addrRslvr + ", addr=" + sockAddr + ']', e);
        }
    }

    /**
     * Returns string representation of node addresses.
     *
     * @param node Grid node.
     * @return String representation of addresses.
     */
    public static String addressesAsString(ClusterNode node) {
        return addressesAsString(node.addresses(), node.hostNames());
    }

    /**
     * Returns string representation of addresses.
     *
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @return String representation of addresses.
     */
    public static String addressesAsString(Collection<String> addrs, Collection<String> hostNames) {
        if (F.isEmpty(addrs))
            return "";

        if (F.isEmpty(hostNames))
            return addrs.toString();

        SB sb = new SB("[");

        Iterator<String> hostNamesIt = hostNames.iterator();

        boolean first = true;

        for (String addr : addrs) {
            if (first)
                first = false;
            else
                sb.a(", ");

            String hostName = hostNamesIt.hasNext() ? hostNamesIt.next() : null;

            sb.a(hostName != null ? hostName : "").a('/').a(addr);
        }

        sb.a(']');

        return sb.toString();
    }

    /**
     * Get default work directory.
     *
     * @return Default work directory.
     */
    public static String defaultWorkDirectory() throws IgniteCheckedException {
        return workDirectory(null, null);
    }

    /**
     * Get work directory for the given user-provided work directory and Ignite home.
     *
     * @param userWorkDir Ignite work folder provided by user.
     * @param userIgniteHome Ignite home folder provided by user.
     */
    public static String workDirectory(@Nullable String userWorkDir, @Nullable String userIgniteHome)
        throws IgniteCheckedException {
        if (userIgniteHome == null)
            userIgniteHome = getIgniteHome();

        File workDir;

        if (!F.isEmpty(userWorkDir))
            workDir = new File(userWorkDir);
        else if (!F.isEmpty(IGNITE_WORK_DIR))
            workDir = new File(IGNITE_WORK_DIR);
        else if (!F.isEmpty(userIgniteHome))
            workDir = new File(userIgniteHome, DEFAULT_WORK_DIR);
        else {
            String userDir = System.getProperty("user.dir");

            if (F.isEmpty(userDir))
                throw new IgniteCheckedException(
                    "Failed to resolve Ignite work directory. Either IgniteConfiguration.setWorkDirectory or " +
                        "one of the system properties (" + IGNITE_HOME + ", " +
                        IgniteSystemProperties.IGNITE_WORK_DIR + ") must be explicitly set."
                );

            File igniteDir = new File(userDir, "ignite");

            try {
                igniteDir.mkdirs();

                File readme = new File(igniteDir, "README.txt");

                if (!readme.exists()) {
                    writeStringToFile(
                        readme,
                        "This is Apache Ignite working directory that contains information that \n" +
                        "    Ignite nodes need in order to function normally.\n" +
                        "Don't delete it unless you're sure you know what you're doing.\n\n" +
                        "You can change the location of working directory with \n" +
                        "    igniteConfiguration.setWorkDirectory(location) or \n" +
                        "    <property name=\"workDirectory\" value=\"location\"/> in IgniteConfiguration <bean>.\n",
                        Charset.defaultCharset().toString(),
                        false);
                }
            }
            catch (Exception ignore) {
                // Ignore.
            }

            workDir = new File(igniteDir, DEFAULT_WORK_DIR);
        }

        if (!workDir.isAbsolute())
            throw new IgniteCheckedException("Work directory path must be absolute: " + workDir);

        if (!mkdirs(workDir))
            throw new IgniteCheckedException("Work directory does not exist and cannot be created: " + workDir);

        if (!workDir.canRead())
            throw new IgniteCheckedException("Cannot read from work directory: " + workDir);

        if (!workDir.canWrite())
            throw new IgniteCheckedException("Cannot write to work directory: " + workDir);

        return workDir.getAbsolutePath();
    }

    /**
     * Nullifies Ignite home directory. For test purposes only.
     */
    public static void nullifyHomeDirectory() {
        ggHome = null;
    }

    /**
     * Resolves work directory.
     *
     * @param workDir Work directory.
     * @param path Path to resolve.
     * @param delIfExist Flag indicating whether to delete the specify directory or not.
     * @return Resolved work directory.
     * @throws IgniteCheckedException If failed.
     */
    public static File resolveWorkDirectory(String workDir, String path, boolean delIfExist) throws IgniteCheckedException {
        return resolveWorkDirectory(workDir, path, delIfExist, true);
    }

    /**
     * Resolves work directory.
     *
     * @param workDir Work directory.
     * @param path Path to resolve.
     * @param delIfExist Flag indicating whether to delete the specify directory or not.
     * @param create If {@code true} then directory must be created if not exists.
     * @return Resolved work directory.
     * @throws IgniteCheckedException If failed.
     */
    public static File resolveWorkDirectory(String workDir, String path, boolean delIfExist, boolean create)
        throws IgniteCheckedException {
        File dir = new File(path);

        if (!dir.isAbsolute()) {
            if (F.isEmpty(workDir))
                throw new IgniteCheckedException("Failed to resolve path (work directory has not been set): " + path);

            dir = new File(workDir, dir.getPath());
        }

        if (delIfExist && dir.exists()) {
            if (!delete(dir))
                throw new IgniteCheckedException("Failed to delete directory: " + dir);
        }

        if (!create)
            return dir;

        if (!mkdirs(dir))
            throw new IgniteCheckedException("Directory does not exist and cannot be created: " + dir);

        if (!dir.canRead())
            throw new IgniteCheckedException("Cannot read from directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteCheckedException("Cannot write to directory: " + dir);

        return dir;
    }

    /**
     * Checks if the given directory exists and attempts to create one if not.
     *
     * @param dir Directory to check.
     * @param msg Directory name for the messages.
     * @param log Optional logger to log a message that the directory has been resolved.
     * @throws IgniteCheckedException If directory does not exist and failed to create it, or if a file with
     *      the same name already exists.
     */
    public static void ensureDirectory(File dir, String msg, IgniteLogger log) throws IgniteCheckedException {
        if (!dir.exists()) {
            if (!dir.mkdirs())
                throw new IgniteCheckedException("Failed to create " + msg + ": " +
                    dir.getAbsolutePath());
        }
        else if (!dir.isDirectory())
            throw new IgniteCheckedException("Failed to initialize " + msg +
                " (a file with the same name already exists): " + dir.getAbsolutePath());

        if (log != null && log.isInfoEnabled())
            log.info("Resolved " + msg + ": " + dir.getAbsolutePath());
    }

    /**
     * Creates {@code IgniteCheckedException} with the collection of suppressed exceptions.
     *
     * @param msg Message.
     * @param suppressed The collections of suppressed exceptions.
     * @return {@code IgniteCheckedException}.
     */
    public static IgniteCheckedException exceptionWithSuppressed(String msg, @Nullable Collection<Throwable> suppressed) {
        IgniteCheckedException e = new IgniteCheckedException(msg);

        if (suppressed != null) {
            for (Throwable th : suppressed)
                e.addSuppressed(th);
        }

        return e;
    }

    /**
     * Extracts full name of enclosing class from JDK8 lambda class name.
     *
     * @param clsName JDK8 lambda class name.
     * @return Full name of enclosing class for JDK8 lambda class name or
     *      {@code null} if passed in name is not related to lambda.
     */
    @Nullable public static String lambdaEnclosingClassName(String clsName) {
        int idx0 = clsName.indexOf("$$Lambda$"); // Java 8+
        int idx1 = clsName.indexOf("$$Lambda/"); // Java 21+

        if (idx0 == idx1)
            return null;

        return clsName.substring(0, idx0 >= 0 ? idx0 : idx1);
    }

    /**
     * Converts a hexadecimal character to an integer.
     *
     * @param ch A character to convert to an integer digit
     * @param idx The index of the character in the source
     * @return An integer
     * @throws IgniteCheckedException Thrown if ch is an illegal hex character
     */
    public static int toDigit(char ch, int idx) throws IgniteCheckedException {
        int digit = Character.digit(ch, 16);

        if (digit == -1)
            throw new IgniteCheckedException("Illegal hexadecimal character " + ch + " at index " + idx);

        return digit;
    }

    /**
     * Gets oldest node out of collection of nodes.
     *
     * @param c Collection of nodes.
     * @return Oldest node.
     */
    public static ClusterNode oldest(Collection<ClusterNode> c, @Nullable IgnitePredicate<ClusterNode> p) {
        ClusterNode oldest = null;

        long minOrder = Long.MAX_VALUE;

        for (ClusterNode n : c) {
            if ((p == null || p.apply(n)) && n.order() < minOrder) {
                oldest = n;

                minOrder = n.order();
            }
        }

        return oldest;
    }

    /**
     * Gets youngest node out of collection of nodes.
     *
     * @param c Collection of nodes.
     * @return Youngest node.
     */
    public static ClusterNode youngest(Collection<ClusterNode> c, @Nullable IgnitePredicate<ClusterNode> p) {
        ClusterNode youngest = null;

        long maxOrder = Long.MIN_VALUE;

        for (ClusterNode n : c) {
            if ((p == null || p.apply(n)) && n.order() > maxOrder) {
                youngest = n;

                maxOrder = n.order();
            }
        }

        return youngest;
    }

    /**
     * @param ptr Address.
     * @param size Size.
     * @return Bytes.
     */
    public static byte[] copyMemory(long ptr, int size) {
        byte[] res = new byte[size];

        GridUnsafe.copyMemory(null, ptr, res, GridUnsafe.BYTE_ARR_OFF, size);

        return res;
    }

    /**
     * Creates new {@link HashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int expSize) {
        return new LinkedHashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link HashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> HashSet<T> newHashSet(int expSize) {
        return new HashSet<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> LinkedHashSet<T> newLinkedHashSet(int expSize) {
        return new LinkedHashSet<>(capacity(expSize));
    }

    /**
     * Creates new map that limited by size.
     *
     * @param limit Limit for size.
     */
    public static <K, V> Map<K, V> limitedMap(int limit) {
        if (limit == 0)
            return Collections.emptyMap();

        if (limit < 5)
            return new GridLeanMap<>(limit);

        return new HashMap<>(capacity(limit), 0.75f);
    }

    /**
     * @param col non-null collection with one element
     * @return a SingletonList containing the element in the original collection
     */
    public static <T> Collection<T> convertToSingletonList(Collection<T> col) {
        if (col.size() != 1) {
            throw new IllegalArgumentException("Unexpected collection size for singleton list, expecting 1 but was: " + col.size());
        }
        return Collections.singletonList(col.iterator().next());
    }

    /**
     * Returns comparator that sorts remote node addresses. If remote node resides on the same host, then put
     * loopback addresses first, last otherwise.
     *
     * @param sameHost {@code True} if remote node resides on the same host, {@code false} otherwise.
     * @return Comparator.
     */
    public static Comparator<InetSocketAddress> inetAddressesComparator(final boolean sameHost) {
        return new Comparator<InetSocketAddress>() {
            @Override public int compare(InetSocketAddress addr1, InetSocketAddress addr2) {
                if (addr1.isUnresolved() && addr2.isUnresolved())
                    return 0;

                if (addr1.isUnresolved() || addr2.isUnresolved())
                    return addr1.isUnresolved() ? 1 : -1;

                boolean addr1Loopback = addr1.getAddress().isLoopbackAddress();

                // No need to reorder.
                if (addr1Loopback == addr2.getAddress().isLoopbackAddress())
                    return 0;

                if (sameHost)
                    return addr1Loopback ? -1 : 1;
                else
                    return addr1Loopback ? 1 : -1;
            }
        };
    }

    /**
     * Finds a method in the class and it parents.
     *
     * Method.getMethod() does not return non-public method,
     * Method.getDeclaratedMethod() does not look at parent classes.
     *
     * @param cls The class to search,
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method findNonPublicMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        while (cls != null) {
            Method mtd = getNonPublicMethod(cls, name, paramTypes);

            if (mtd != null)
                return mtd;

            cls = cls.getSuperclass();
        }

        return null;
    }

    /**
     * Gets a method from the class.
     *
     * Method.getMethod() does not return non-public method.
     *
     * @param cls Target class.
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method getNonPublicMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        try {
            Method mtd = cls.getDeclaredMethod(name, paramTypes);

            mtd.setAccessible(true);

            return mtd;
        }
        catch (NoSuchMethodException ignored) {
            // No-op.
        }

        return null;
    }

    /**
     * Finds a non-static and non-abstract method from the class it parents.
     *
     * Method.getMethod() does not return non-public method.
     *
     * @param cls Target class.
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method findInheritableMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        Method mtd = null;

        Class<?> cls0 = cls;

        while (cls0 != null) {
            try {
                mtd = cls0.getDeclaredMethod(name, paramTypes);

                break;
            }
            catch (NoSuchMethodException e) {
                cls0 = cls0.getSuperclass();
            }
        }

        if (mtd == null)
            return null;

        mtd.setAccessible(true);

        int mods = mtd.getModifiers();

        if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0)
            return null;
        else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0)
            return mtd;
        else if ((mods & Modifier.PRIVATE) != 0)
            return cls == cls0 ? mtd : null;
        else {
            ClassLoader clsLdr = cls.getClassLoader();

            ClassLoader clsLdr0 = cls0.getClassLoader();

            return clsLdr == clsLdr0 && packageName(cls).equals(packageName(cls0)) ? mtd : null;
        }
    }

    /**
     * @param cls Class.
     * @return Package name.
     */
    public static String packageName(Class<?> cls) {
        Package pkg = cls.getPackage();

        return pkg == null ? "" : pkg.getName();
    }

    /**
     * @param cls The class to search.
     * @param name Name of a field to get.
     * @return Field or {@code null}.
     */
    @Nullable public static Field findField(Class<?> cls, String name) {
        while (cls != null) {
            try {
                Field fld = cls.getDeclaredField(name);

                if (!fld.isAccessible())
                    fld.setAccessible(true);

                return fld;
            }
            catch (NoSuchFieldException ignored) {
                // No-op.
            }

            cls = cls.getSuperclass();
        }

        return null;
    }

    /**
     * @param c Collection.
     * @param p Optional filters.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Collection<T> c, @Nullable IgnitePredicate<? super T>... p) {
        assert c != null;

        return arrayList(c.iterator(), c.size(), p);
    }

    /**
     * @param c Collection.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Collection<T> c) {
        assert c != null;

        return new ArrayList<R>(c);
    }

    /**
     * @param c Collection.
     * @param cap Initial capacity.
     * @param p Optional filters.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Iterator<T> c, int cap,
        @Nullable IgnitePredicate<? super T>... p) {
        assert c != null;
        assert cap >= 0;

        List<R> list = new ArrayList<>(cap);

        while (c.hasNext()) {
            T t = c.next();

            if (F.isAll(t, p))
                list.add(t);
        }

        return list;
    }

    /**
     * Fully writes communication message to provided stream.
     *
     * @param msg Message.
     * @param out Stream to write to.
     * @param buf Byte buffer that will be passed to {@link Message#writeTo(ByteBuffer, MessageWriter)} method.
     * @param writer Message writer.
     * @return Number of written bytes.
     * @throws IOException In case of error.
     */
    public static int writeMessageFully(Message msg, OutputStream out, ByteBuffer buf,
        MessageWriter writer) throws IOException {
        assert msg != null;
        assert out != null;
        assert buf != null;
        assert buf.hasArray();

        boolean finished = false;
        int cnt = 0;

        while (!finished) {
            finished = msg.writeTo(buf, writer);

            out.write(buf.array(), 0, buf.position());

            cnt += buf.position();

            buf.clear();
        }

        return cnt;
    }

    /**
     * Throws exception with uniform error message if given parameter's assertion condition
     * is {@code false}.
     *
     * @param cond Assertion condition to check.
     * @param condDesc Description of failed condition.
     */
    public static void assertParameter(boolean cond, String condDesc) throws IgniteException {
        if (!cond)
            throw new IgniteException("Parameter failed condition check: " + condDesc);
    }

    /**
     * @param lock Lock.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public static void writeLock(ReadWriteLock lock) throws IgniteInterruptedCheckedException {
        try {
            lock.writeLock().lockInterruptibly();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * @return Whether provided method is {@code Object.hashCode()}.
     */
    public static boolean isHashCodeMethod(Method mtd) {
        return hashCodeMtd.equals(mtd);
    }

    /**
     * @return Whether provided method is {@code Object.equals(...)}.
     */
    public static boolean isEqualsMethod(Method mtd) {
        return equalsMtd.equals(mtd);
    }

    /**
     * @return Whether provided method is {@code Object.toString()}.
     */
    public static boolean isToStringMethod(Method mtd) {
        return toStringMtd.equals(mtd);
    }

    /**
     * @param threadId Thread ID.
     * @return Thread name if found.
     */
    public static String threadName(long threadId) {
        Thread[] threads = new Thread[Thread.activeCount()];

        int cnt = Thread.enumerate(threads);

        for (int i = 0; i < cnt; i++)
            if (threads[i].getId() == threadId)
                return threads[i].getName();

        return "<failed to find active thread " + threadId + '>';
    }

    /**
     * @param t0 Comparable object.
     * @param t1 Comparable object.
     * @param <T> Comparable type.
     * @return Maximal object o t0 and t1.
     */
    public static <T extends Comparable<? super T>> T max(T t0, T t1) {
        return t0.compareTo(t1) > 0 ? t0 : t1;
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param in Input stream.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(Marshaller marsh, InputStream in, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert marsh != null;
        assert in != null;

        try {
            return marsh.unmarshal(in, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param marsh Marshaller.
     * @param zipBytes Zip-compressed bytes.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshalZip(Marshaller marsh, byte[] zipBytes, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        assert marsh != null;
        assert zipBytes != null;

        try {
            ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zipBytes));

            in.getNextEntry();

            return marsh.unmarshal(in, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param marsh Marshaller.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(Marshaller marsh, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert marsh != null;
        assert arr != null;

        try {
            return marsh.unmarshal(arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param ctx Kernal contex.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(GridKernalContext ctx, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert ctx != null;
        assert arr != null;

        try {
            return unmarshal(ctx.marshaller(), arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param ctx Kernal contex.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(GridCacheSharedContext ctx, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert ctx != null;
        assert arr != null;

        try {
            return unmarshal(ctx.marshaller(), arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(Marshaller marsh, Object obj) throws IgniteCheckedException {
        assert marsh != null;

        try {
            return marsh.marshal(obj);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @param out Output stream.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static void marshal(Marshaller marsh, @Nullable Object obj, OutputStream out)
        throws IgniteCheckedException {
        assert marsh != null;

        try {
            marsh.marshal(obj, out);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array. Wrap marshaller
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param ctx Kernal context.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridKernalContext ctx, Object obj) throws IgniteCheckedException {
        assert ctx != null;

        return marshal(ctx.marshaller(), obj);
    }

    /**
     * Marshals object to byte array. Wrap marshaller
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param ctx Cache context.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridCacheSharedContext ctx, Object obj) throws IgniteCheckedException {
        assert ctx != null;

        return marshal(ctx.marshaller(), obj);
    }

    /**
     * Get current Ignite name.
     *
     * @return Current Ignite name.
     */
    @Nullable public static String getCurrentIgniteName() {
        return LOC_IGNITE_NAME.get();
    }

    /**
     * Check if current Ignite name is set.
     *
     * @param name Name to check.
     * @return {@code True} if set.
     */
    @SuppressWarnings("StringEquality")
    public static boolean isCurrentIgniteNameSet(@Nullable String name) {
        return name != LOC_IGNITE_NAME_EMPTY;
    }

    /**
     * Set current Ignite name.
     *
     * @param newName New name.
     * @return Old name.
     */
    @SuppressWarnings("StringEquality")
    @Nullable public static String setCurrentIgniteName(@Nullable String newName) {
        String oldName = LOC_IGNITE_NAME.get();

        if (oldName != newName)
            LOC_IGNITE_NAME.set(newName);

        return oldName;
    }

    /**
     * Restore old Ignite name.
     *
     * @param oldName Old name.
     * @param curName Current name.
     */
    @SuppressWarnings("StringEquality")
    public static void restoreOldIgniteName(@Nullable String oldName, @Nullable String curName) {
        if (oldName != curName)
            LOC_IGNITE_NAME.set(oldName);
    }

    /**
     * Zip binary payload using default compression.
     *
     * @param bytes Byte array to compress.
     * @return Compressed bytes.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] zip(@Nullable byte[] bytes) throws IgniteCheckedException {
        return zip(bytes, Deflater.DEFAULT_COMPRESSION);
    }

    /**
     * @param bytes Byte array to compress.
     * @param compressionLevel Level of compression to encode.
     * @return Compressed bytes.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] zip(@Nullable byte[] bytes, int compressionLevel) throws IgniteCheckedException {
        try {
            if (bytes == null)
                return null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try (ZipOutputStream zos = new ZipOutputStream(bos)) {
                zos.setLevel(compressionLevel);

                ZipEntry entry = new ZipEntry("");

                try {
                    entry.setSize(bytes.length);

                    zos.putNextEntry(entry);

                    zos.write(bytes);
                }
                finally {
                    zos.closeEntry();
                }
            }

            return bos.toByteArray();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Serialize object to byte array.
     *
     * @param obj Object.
     * @return Serialized object.
     */
    public static byte[] toBytes(Serializable obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(obj);
            oos.flush();

            return bos.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Deserialize object from byte array.
     *
     * @param data Serialized object.
     * @return Object.
     */
    public static <T> T fromBytes(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {

            return (T)ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Get checkpoint buffer size for the given configuration.
     *
     * @param regCfg Configuration.
     * @return Checkpoint buffer size.
     */
    public static long checkpointBufferSize(DataStorageConfiguration dsCfg, DataRegionConfiguration regCfg) {
        if (!regCfg.isPersistenceEnabled())
            return 0L;

        long res = regCfg.getCheckpointPageBufferSize();

        if (res == 0L) {
            long maxCpPageBufSize = dsCfg.isWriteRecoveryDataOnCheckpoint() ?
                DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE_CP_RECOVERY :
                DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE_WAL_RECOVERY;

            if (regCfg.getMaxSize() < GB)
                res = Math.min(DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE, regCfg.getMaxSize());
            else
                res = Math.min(regCfg.getMaxSize() / 4, maxCpPageBufSize);
        }

        return res;
    }

    /**
     * Calculates maximum WAL archive size based on maximum checkpoint buffer size, if the default value of
     * {@link DataStorageConfiguration#getMaxWalArchiveSize()} is not overridden.
     *
     * @return User-set max WAL archive size of triple size of the maximum checkpoint buffer.
     */
    public static long adjustedWalHistorySize(DataStorageConfiguration dsCfg, @Nullable IgniteLogger log) {
        if (dsCfg.getMaxWalArchiveSize() != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE &&
            dsCfg.getMaxWalArchiveSize() != DataStorageConfiguration.DFLT_WAL_ARCHIVE_MAX_SIZE)
            return dsCfg.getMaxWalArchiveSize();

        // Find out the maximum checkpoint buffer size.
        long maxCpBufSize = 0;

        if (dsCfg.getDataRegionConfigurations() != null) {
            for (DataRegionConfiguration regCfg : dsCfg.getDataRegionConfigurations()) {
                long cpBufSize = checkpointBufferSize(dsCfg, regCfg);

                if (cpBufSize > regCfg.getMaxSize())
                    cpBufSize = regCfg.getMaxSize();

                if (cpBufSize > maxCpBufSize)
                    maxCpBufSize = cpBufSize;
            }
        }

        {
            DataRegionConfiguration regCfg = dsCfg.getDefaultDataRegionConfiguration();

            long cpBufSize = checkpointBufferSize(dsCfg, regCfg);

            if (cpBufSize > regCfg.getMaxSize())
                cpBufSize = regCfg.getMaxSize();

            if (cpBufSize > maxCpBufSize)
                maxCpBufSize = cpBufSize;
        }

        long adjustedWalArchiveSize = maxCpBufSize * 4;

        if (adjustedWalArchiveSize > dsCfg.getMaxWalArchiveSize()) {
            if (log != null)
                quietAndInfo(log, "Automatically adjusted max WAL archive size to " +
                    readableSize(adjustedWalArchiveSize, false) +
                    " (to override, use DataStorageConfiguration.setMaxWalArchiveSize)");

            return adjustedWalArchiveSize;
        }

        return dsCfg.getMaxWalArchiveSize();
    }

    /**
     * Return count of regular file in the directory (including in sub-directories)
     *
     * @param dir path to directory
     * @return count of regular file
     * @throws IOException sometimes
     */
    public static int fileCount(Path dir) throws IOException {
        int cnt = 0;

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path d : ds) {
                if (Files.isDirectory(d))
                    cnt += fileCount(d);

                else if (Files.isRegularFile(d))
                    cnt++;
            }
        }

        return cnt;
    }

    /**
     * Puts additional text to thread name.
     * Calls {@code enhanceThreadName(Thread.currentThread(), text)}.
     * For details see {@link #enhanceThreadName(Thread, String)}.
     *
     * @param text Text to be set in thread name in square [] braces. Does nothing if cannot find suitable braces.
     */
    public static void enhanceThreadName(String text) {
        enhanceThreadName(Thread.currentThread(), text);
    }

    /**
     * Puts additional text to thread name. It finds first square braces in thread name and
     * sets required text in them. For example, thread has name "my-thread-[]-%gridname%". After
     * calling {@code enhanceThreadName(thread, "myText")}, the name of the thread will
     * be "my-thread-[myText]-%gridname%".<br>
     * This allows to set additional mutable info to thread, like remote host IP address or so.
     *
     * @param thread Thread to be renamed, it must contain square braces in name []. Does nothing if {@code null}.
     * @param text Text to be set in thread name in square [] braces. Does nothing if cannot find suitable braces.
     */
    public static void enhanceThreadName(@Nullable Thread thread, String text) {
        if (thread == null)
            return;

        String threadName = thread.getName();

        int idxStart = threadName.indexOf('[');
        int idxEnd = threadName.indexOf(']');

        if (idxStart < 0 || idxEnd < 0 || idxStart >= idxEnd)
            return;

        StringBuilder sb = new StringBuilder(threadName.length());

        sb.append(threadName, 0, idxStart + 1);
        sb.append(text);
        sb.append(threadName, idxEnd, threadName.length());

        thread.setName(sb.toString());
    }

    /**
     * Check that node Ignite product version is not less then specified.
     *
     * @param ver Target Ignite product version.
     * @param nodes Cluster nodes.
     * @return {@code True} if ignite product version of all nodes is not less then {@code ver}.
     */
    public static boolean isOldestNodeVersionAtLeast(IgniteProductVersion ver, Iterable<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (node.version().compareToIgnoreTimestamp(ver) < 0)
                return false;
        }

        return true;
    }

    /**
     * @param addr pointer in memory
     * @param len how much byte to read (should divide 8)
     *
     * @return hex representation of memory region
     */
    public static String toHexString(long addr, int len) {
        StringBuilder sb = new StringBuilder(len * 2);

        for (int i = 0; i < len; i++) // Can not use getLong because on little-endian it produces bs.
            addByteAsHex(sb, GridUnsafe.getByte(addr + i));

        return sb.toString();
    }

    /**
     * @param buf which content should be converted to string
     *
     * @return hex representation of memory region
     */
    public static String toHexString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder(buf.capacity() * 2);

        for (int i = 0; i < buf.capacity(); i++)
            addByteAsHex(sb, buf.get(i)); // Can not use getLong because on little-endian it produces bs.

        return sb.toString();
    }

    /**
     * @param ctx Kernel context.
     * @return Random alive server node.
     */
    public static ClusterNode randomServerNode(GridKernalContext ctx) {
        Collection<ClusterNode> aliveNodes = ctx.discovery().aliveServerNodes();

        int rndIdx = RND.nextInt(aliveNodes.size()) + 1;

        int i = 0;
        ClusterNode rndNode = null;

        for (Iterator<ClusterNode> it = aliveNodes.iterator(); i < rndIdx && it.hasNext(); i++)
            rndNode = it.next();

        if (rndNode == null)
            assert rndNode != null;

        return rndNode;
    }

    /**
     * @param ctx Kernal context.
     * @param plc IO Policy.
     * @param reserved Thread to reserve.
     * @return Number of available threads in executor service for {@code plc}. If {@code plc}
     *         is invalid, return {@code 1}.
     */
    public static int availableThreadCount(GridKernalContext ctx, byte plc, int reserved) {
        IgniteConfiguration cfg = ctx.config();

        int parallelismLvl;

        switch (plc) {
            case GridIoPolicy.P2P_POOL:
                parallelismLvl = cfg.getPeerClassLoadingThreadPoolSize();

                break;

            case GridIoPolicy.SYSTEM_POOL:
                parallelismLvl = cfg.getSystemThreadPoolSize();

                break;

            case GridIoPolicy.PUBLIC_POOL:
                parallelismLvl = cfg.getPublicThreadPoolSize();

                break;

            case GridIoPolicy.MANAGEMENT_POOL:
                parallelismLvl = cfg.getManagementThreadPoolSize();

                break;

            case GridIoPolicy.UTILITY_CACHE_POOL:
                parallelismLvl = cfg.getUtilityCacheThreadPoolSize();

                break;

            case GridIoPolicy.SERVICE_POOL:
                parallelismLvl = cfg.getServiceThreadPoolSize();

                break;

            case GridIoPolicy.DATA_STREAMER_POOL:
                parallelismLvl = cfg.getDataStreamerThreadPoolSize();

                break;

            case GridIoPolicy.QUERY_POOL:
                parallelismLvl = cfg.getQueryThreadPoolSize();

                break;

            default:
                parallelismLvl = -1;
        }

        return Math.max(1, parallelismLvl - reserved);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallel(
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(srcDatas.size(), executorSvc, srcDatas, operation);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallel(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(parallelismLvl, executorSvc, srcDatas, operation, false);
    }

    /**
     * Execute operation on data in parallel uninterruptibly.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallelUninterruptibly(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(parallelismLvl, executorSvc, srcDatas, operation, true);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @param uninterruptible {@code true} if a result should be awaited in any case.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    private static <T, R> Collection<R> doInParallel(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation,
        boolean uninterruptible
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        if (srcDatas.isEmpty())
            return Collections.emptyList();

        int[] batchSizes = calculateOptimalBatchSizes(parallelismLvl, srcDatas.size());

        List<Batch<T, R>> batches = new ArrayList<>(batchSizes.length);

        // Set for sharing batches between executor and current thread.
        // If executor cannot perform immediately, we will execute task in the current thread.
        Set<Batch<T, R>> sharedBatchesSet = new GridConcurrentHashSet<>(batchSizes.length);

        Iterator<T> iter = srcDatas.iterator();

        for (int idx = 0; idx < batchSizes.length; idx++) {
            int batchSize = batchSizes[idx];

            Batch<T, R> batch = new Batch<>(batchSize, uninterruptible);

            for (int i = 0; i < batchSize; i++)
                batch.addTask(iter.next());

            batches.add(batch);
        }

        batches = batches.stream()
            .filter(batch -> !batch.tasks.isEmpty())
            // Add to set only after check that batch is not empty.
            .peek(sharedBatchesSet::add)
            // Setup future in batch for waiting result.
            .peek(batch -> batch.fut = executorSvc.submit(() -> {
                // Batch was stolen by the main stream.
                if (!sharedBatchesSet.remove(batch))
                    return null;

                Collection<R> results = new ArrayList<>(batch.tasks.size());

                for (T item : batch.tasks)
                    results.add(operation.apply(item));

                return results;
            }))
            .collect(Collectors.toList());

        Throwable error = null;

        // Stealing jobs if executor is busy and cannot process task immediately.
        // Perform batches in a current thread.
        for (Batch<T, R> batch : sharedBatchesSet) {
            // Executor steal task.
            if (!sharedBatchesSet.remove(batch))
                continue;

            Collection<R> res = new ArrayList<>(batch.tasks.size());

            try {
                for (T item : batch.tasks)
                    res.add(operation.apply(item));

                batch.result(res);
            }
            catch (Throwable e) {
                batch.result(e);
            }
        }

        // Final result collection.
        Collection<R> results = new ArrayList<>(srcDatas.size());

        for (Batch<T, R> batch : batches) {
            try {
                Throwable err = batch.error;

                if (err != null) {
                    error = addSuppressed(error, err);

                    continue;
                }

                Collection<R> res = batch.result();

                if (res != null)
                    results.addAll(res);
                else
                    assert error != null;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }
            catch (ExecutionException e) {
                error = addSuppressed(error, e.getCause());
            }
            catch (CancellationException e) {
                error = addSuppressed(error, e);
            }
        }

        if (error != null) {
            if (error instanceof IgniteCheckedException)
                throw (IgniteCheckedException)error;

            if (error instanceof RuntimeException)
                throw (RuntimeException)error;

            if (error instanceof Error)
                throw (Error)error;

            throw new IgniteCheckedException(error);
        }

        return results;
    }

    /**
     * Utility method to add the given throwable error to the given throwable root error. If the given
     * suppressed throwable is an {@code Error}, but the root error is not, will change the root to the {@code Error}.
     *
     * @param root Root error to add suppressed error to.
     * @param err Error to add.
     * @return New root error.
     */
    public static <T extends Throwable> T addSuppressed(T root, T err) {
        assert err != null;

        if (root == null)
            return err;

        if (err instanceof Error && !(root instanceof Error)) {
            err.addSuppressed(root);

            root = err;
        }
        else
            root.addSuppressed(err);

        return root;
    }

    /**
     * @return {@code true} if local node is coordinator.
     */
    public static boolean isLocalNodeCoordinator(GridDiscoveryManager discoMgr) {
        if (discoMgr.localNode().isClient())
            return false;

        DiscoverySpi spi = discoMgr.getInjectedDiscoverySpi();

        return spi instanceof TcpDiscoverySpi
            ? ((TcpDiscoverySpi)spi).isLocalNodeCoordinator()
            : Objects.equals(discoMgr.localNode(), oldest(discoMgr.aliveServerNodes(), null));
    }

    /**
     * The batch of tasks with a batch index in global array.
     */
    private static class Batch<T, R> {
        /** List tasks. */
        private final List<T> tasks;

        /** */
        private Collection<R> result;

        /** */
        private Throwable error;

        /** */
        private Future<Collection<R>> fut;

        /** */
        private final boolean uninterruptible;

        /**
         * @param batchSize Batch size.
         * @param uninterruptible {@code true} if a result should be awaited in any case.
         */
        private Batch(int batchSize, boolean uninterruptible) {
            tasks = new ArrayList<>(batchSize);

            this.uninterruptible = uninterruptible;
        }

        /**
         * @param task Add task.
         */
        public void addTask(T task) {
            tasks.add(task);
        }

        /**
         * @param res Setup results for tasks.
         */
        public void result(Collection<R> res) {
            result = res;
        }

        /**
         * @param e Throwable if task was completed with error.
         */
        public void result(Throwable e) {
            this.error = e;
        }

        /**
         * Get tasks results.
         */
        public Collection<R> result() throws ExecutionException, InterruptedException {
            assert fut != null;

            if (result != null)
                return result;

            return uninterruptible ? getUninterruptibly(fut) : fut.get();
        }
    }

    /**
     * Split number of tasks into optimized batches.
     * @param parallelismLvl Level of parallelism.
     * @param size number of tasks to split.
     * @return array of batch sizes.
     */
    public static int[] calculateOptimalBatchSizes(int parallelismLvl, int size) {
        int[] batcheSizes = new int[Math.min(parallelismLvl, size)];

        for (int i = 0; i < size; i++)
            batcheSizes[i % batcheSizes.length]++;

        return batcheSizes;
    }

    /**
     * @param fut Future to wait for completion.
     * @throws ExecutionException If the future
     */
    private static <R> R getUninterruptibly(Future<R> fut) throws ExecutionException {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return fut.get();
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     *
     * @param r Runnable.
     * @param fut Grid future apater.
     * @return Runnable with wrapped future.
     */
    public static Runnable wrapIgniteFuture(Runnable r, GridFutureAdapter<?> fut) {
        return () -> {
            try {
                r.run();

                fut.onDone();
            }
            catch (Throwable e) {
                fut.onDone(e);

                throw e;
            }
        };
    }

    /**
     *  Safely write buffer fully to blocking socket channel.
     *  Will throw assert if non blocking channel passed.
     *
     * @param sockCh WritableByteChannel.
     * @param buf Buffer.
     * @throws IOException IOException.
     */
    public static void writeFully(SocketChannel sockCh, ByteBuffer buf) throws IOException {
        int totalWritten = 0;

        assert sockCh.isBlocking() : "SocketChannel should be in blocking mode " + sockCh;

        while (buf.hasRemaining()) {
            int written = sockCh.write(buf);

            if (written < 0)
                throw new IOException("Error writing buffer to channel " +
                    "[written = " + written + ", buf " + buf + ", totalWritten = " + totalWritten + "]");

            totalWritten += written;
        }
    }

    /**
     * @return New identity hash set.
     */
    public static <X> Set<X> newIdentityHashSet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * @param stripes Number of stripes.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @return Stripe idx.
     */
    public static int stripeIdx(int stripes, int grpId, int partId) {
        assert partId >= 0;

        return Math.abs((Math.abs(grpId) + partId)) % stripes;
    }

    /**
     * Notifies provided {@code lsnrs} with the value {@code t}.
     *
     * @param t Consumed object.
     * @param lsnrs Listeners.
     * @param <T> Type of consumed object.
     */
    public static <T> void notifyListeners(T t, Collection<Consumer<T>> lsnrs, IgniteLogger log) {
        if (lsnrs == null)
            return;

        for (Consumer<T> lsnr : lsnrs) {
            try {
                lsnr.accept(t);
            }
            catch (Exception e) {
                warn(log, "Listener error", e);
            }
        }
    }

    /**
     * Stops workers from given collection and waits for their completion.
     *
     * @param workers Workers collection.
     * @param cancel Wheter should cancel workers.
     * @param log Logger.
     */
    public static void awaitForWorkersStop(
        Collection<GridWorker> workers,
        boolean cancel,
        @Nullable IgniteLogger log
    ) {
        for (GridWorker worker : workers) {
            try {
                if (cancel)
                    worker.cancel();

                worker.join();
            }
            catch (Exception e) {
                if (log != null)
                    log.warning("Failed to cancel grid runnable [" + worker.toString() + "]: " + e.getMessage());
            }
        }
    }

    /**
     * Writes string to output stream accounting for {@code null} values. <br/>
     *
     * This method can write string of any length, limit of <code>65535</code> is not applied.
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeLongString(DataOutput out, @Nullable String s) throws IOException {
        // Write null flag.
        out.writeBoolean(isNull(s));

        if (isNull(s))
            return;

        int sLen = s.length();

        // Write string length.
        out.writeInt(sLen);

        // Write byte array.
        for (int i = 0; i < sLen; i++) {
            char c = s.charAt(i);
            int utfBytes = utfBytes(c);

            if (utfBytes == 1)
                out.writeByte((byte)c);
            else if (utfBytes == 3) {
                out.writeByte((byte)(0xE0 | (c >> 12) & 0x0F));
                out.writeByte((byte)(0x80 | (c >> 6) & 0x3F));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
            else {
                out.writeByte((byte)(0xC0 | ((c >> 6) & 0x1F)));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Reads string from input stream accounting for {@code null} values. <br/>
     *
     * This method can read string of any length, limit of <code>65535</code> is not applied.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readLongString(DataInput in) throws IOException {
        // Check null value.
        if (in.readBoolean())
            return null;

        // Read string length.
        int sLen = in.readInt();

        StringBuilder strBuilder = new StringBuilder(sLen);

        // Read byte array.
        for (int i = 0, b0, b1, b2; i < sLen; i++) {
            b0 = in.readByte() & 0xff;

            switch (b0 >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:   // 1 byte format: 0xxxxxxx
                    strBuilder.append((char)b0);
                    break;

                case 12:
                case 13:  // 2 byte format: 110xxxxx 10xxxxxx
                    b1 = in.readByte();

                    if ((b1 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x1F) << 6) | (b1 & 0x3F)));
                    break;

                case 14:  // 3 byte format: 1110xxxx 10xxxxxx 10xxxxxx
                    b1 = in.readByte();
                    b2 = in.readByte();

                    if ((b1 & 0xC0) != 0x80 || (b2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F)));
                    break;

                default:  // 10xx xxxx, 1111 xxxx
                    throw new UTFDataFormatException();
            }
        }

        return strBuilder.toString();
    }

    /**
     * Get number of bytes for {@link DataOutput#writeUTF},
     * depending on character: <br/>
     *
     * One byte - If a character <code>c</code> is in the range
     * <code>&#92;u0001</code> through <code>&#92;u007f</code>.<br/>
     *
     * Two bytes - If a character <code>c</code> is <code>&#92;u0000</code> or
     * is in the range <code>&#92;u0080</code> through <code>&#92;u07ff</code>.
     * <br/>
     *
     * Three bytes - If a character <code>c</code> is in the range
     * <code>&#92;u0800</code> through <code>uffff</code>.
     *
     * @param c Character.
     * @return Number of bytes.
     */
    public static int utfBytes(char c) {
        return (c >= 0x0001 && c <= 0x007F) ? 1 : (c > 0x07FF) ? 3 : 2;
    }

    /**
     * Reads string-to-string map written by {@link #writeStringMap(DataOutput, Map)}.
     *
     * @param in Data input.
     * @throws IOException If write failed.
     * @return Read result.
     */
    public static Map<String, String> readStringMap(DataInput in) throws IOException {
        int size = in.readInt();

        if (size == -1)
            return null;
        else {
            Map<String, String> map = newHashMap(size);

            for (int i = 0; i < size; i++)
                map.put(readUTF(in), readUTF(in));

            return map;
        }
    }

    /**
     * Writes string-to-string map to given data output.
     *
     * @param out Data output.
     * @param map Map.
     * @throws IOException If write failed.
     */
    public static void writeStringMap(DataOutput out, @Nullable Map<String, String> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<String, String> e : map.entrySet()) {
                writeUTF(out, e.getKey());
                writeUTF(out, e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /** Maximum string length to be written at once. */
    private static final int MAX_STR_LEN = 0xFFFF / 4;

    /**
     * Write UTF string which can be {@code null}.
     *
     * @param out Output stream.
     * @param val Value.
     * @throws IOException If failed.
     */
    public static void writeUTF(DataOutput out, @Nullable String val) throws IOException {
        if (val == null)
            out.writeInt(-1);
        else {
            out.writeInt(val.length());

            if (val.length() <= MAX_STR_LEN)
                out.writeUTF(val); // Optimized write in 1 chunk.
            else {
                int written = 0;

                while (written < val.length()) {
                    int partLen = Math.min(val.length() - written, MAX_STR_LEN);

                    String part = val.substring(written, written + partLen);

                    out.writeUTF(part);

                    written += partLen;
                }
            }
        }
    }

    /**
     * Read UTF string which can be {@code null}.
     *
     * @param in Input stream.
     * @return Value.
     * @throws IOException If failed.
     */
    public static String readUTF(DataInput in) throws IOException {
        int len = in.readInt(); // May be zero.

        if (len < 0)
            return null;
        else {
            if (len <= MAX_STR_LEN)
                return in.readUTF();

            StringBuilder sb = new StringBuilder(len);

            do {
                sb.append(in.readUTF());
            }
            while (sb.length() < len);

            assert sb.length() == len;

            return sb.toString();
        }
    }

    /** Explicit class for {@code Supplier<Enumeration<NetworkInterface>>}. */
    @FunctionalInterface
    public interface InterfaceSupplier {
        /** Return collection of local network interfaces. */
        Enumeration<NetworkInterface> getInterfaces() throws SocketException;
    }

    /**
     * Converts count of bytes to a human-readable format.
     * Examples: 10 -> 10.0 B, 2048 -> 2.0 KB, etc.
     *
     * @param bytes Byte count.
     * @return Human readable format for count of bytes.
     */
    public static String humanReadableByteCount(long bytes) {
        long base = 1024L;

        int exponent = max((int)(Math.log(bytes) / Math.log(base)), 0);
        String unit = String.valueOf(BYTE_CNT_PREFIXES.charAt(exponent)).trim();

        return String.format((Locale)null, "%.1f %sB", (bytes / Math.pow(base, exponent)), unit);
    }

    /**
     * Converts duration to a human-readable format.
     * Examples: 10 -> 10ms, 6_0000 -> 6s, 65_000 -> 1m5s, (65 * 60_000 + 32_000) -> 1h5m32s, etc.
     *
     * @param millis Duration in milliseconds.
     * @return Human readable format for duration.
     */
    public static String humanReadableDuration(long millis) {
        StringBuilder sb = new StringBuilder();

        if (millis < 0) {
            sb.append('-');

            millis = -millis;
        }

        if (millis < 1_000)
            sb.append(millis).append("ms");
        else {
            long days = TimeUnit.MILLISECONDS.toDays(millis);

            if (days > 0) {
                sb.append(days).append('d');

                millis -= TimeUnit.DAYS.toMillis(days);
            }

            long hours = TimeUnit.MILLISECONDS.toHours(millis);

            if (hours > 0) {
                sb.append(hours).append('h');

                millis -= TimeUnit.HOURS.toMillis(hours);
            }

            long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);

            if (minutes > 0) {
                sb.append(minutes).append('m');

                millis -= TimeUnit.MINUTES.toMillis(minutes);
            }

            long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

            if (seconds > 0)
                sb.append(seconds).append('s');
        }

        return sb.toString();
    }

    /**
     * Getting the total size of uncompressed data in zip.
     *
     * @param zip Zip file.
     * @return Total uncompressed size.
     * @throws IOException If failed.
     */
    public static long uncompressedSize(File zip) throws IOException {
        try (ZipFile zipFile = new ZipFile(zip)) {
            long size = 0;

            Enumeration<? extends ZipEntry> entries = zipFile.entries();

            while (entries.hasMoreElements())
                size += entries.nextElement().getSize();

            return size;
        }
    }

    /**
     * Maps object hash to some index between 0 and specified size via modulo operation.
     *
     * @param hash Object hash.
     * @param size Size greater than 0.
     * @return Calculated index in range [0..size).
     */
    public static int hashToIndex(int hash, int size) {
        return safeAbs(hash % size);
    }

    /**
     * Invokes {@link ServerSocket#accept()} method on the passed server socked, working around the
     * https://bugs.openjdk.java.net/browse/JDK-8247750 in the process.
     *
     * @param srvrSock Server socket.
     * @return New socket.
     * @throws IOException If an I/O error occurs when waiting for a connection.
     * @see ServerSocket#accept()
     */
    public static Socket acceptServerSocket(ServerSocket srvrSock) throws IOException {
        while (true) {
            try {
                return srvrSock.accept();
            }
            catch (SocketTimeoutException e) {
                if (srvrSock.getSoTimeout() > 0)
                    throw e;
            }
        }
    }

    /**
     * @return Language runtime.
     */
    public static String language(ClassLoader ldr) {
        boolean scala = false;
        boolean groovy = false;
        boolean clojure = false;

        for (StackTraceElement elem : Thread.currentThread().getStackTrace()) {
            String s = elem.getClassName().toLowerCase();

            if (s.contains("scala")) {
                scala = true;

                break;
            }
            else if (s.contains("groovy")) {
                groovy = true;

                break;
            }
            else if (s.contains("clojure")) {
                clojure = true;

                break;
            }
        }

        if (scala) {
            try (InputStream in = ldr.getResourceAsStream("/library.properties")) {
                Properties props = new Properties();

                if (in != null)
                    props.load(in);

                return "Scala ver. " + props.getProperty("version.number", "<unknown>");
            }
            catch (Exception ignore) {
                return "Scala ver. <unknown>";
            }
        }

        // How to get Groovy and Clojure version at runtime?!?
        return groovy ? "Groovy" : clojure ? "Clojure" : jdkName + " ver. " + jdkVersion();
    }

    /**
     * @return {@code True} if remote JMX management is enabled - {@code false} otherwise.
     */
    public static boolean isJmxRemoteEnabled() {
        return IGNITE_JMX_REMOTE_PROPERTY != null;
    }

    /**
     * @return {@code true} if the REST processor is enabled, {@code false} the otherwise.
     */
    public static boolean isRestEnabled(IgniteConfiguration cfg) {
        boolean isClientNode = cfg.isClientMode();

        // By default, rest processor doesn't start on client nodes.
        return cfg.getConnectorConfiguration() != null &&
            (!isClientNode || (isClientNode && IgniteSystemProperties.getBoolean(IGNITE_REST_START_ON_CLIENT)));
    }

    /**
     * @return {@code True} if restart mode is enabled, {@code false} otherwise.
     */
    public static boolean isRestartEnabled() {
        return IGNITE_SUCCESS_FILE_PROPERTY != null;
    }

    /**
     * Returns {@code true} if class is a lambda.
     *
     * @param objectClass Class.
     * @return {@code true} if class is a lambda, {@code false} otherwise.
     */
    public static boolean isLambda(Class<?> objectClass) {
        return !objectClass.isPrimitive() && !objectClass.isArray()
            // Order is crucial here, isAnonymousClass and isLocalClass may fail if
            // class' outer class was loaded with different classloader.
            && objectClass.isSynthetic()
            && !objectClass.isAnonymousClass() && !objectClass.isLocalClass()
            && classCannotBeLoadedByName(objectClass);
    }

    /**
     * Returns {@code true} if class can not be loaded by name.
     *
     * @param objectClass Class.
     * @return {@code true} if class can not be loaded by name, {@code false} otherwise.
     */
    public static boolean classCannotBeLoadedByName(Class<?> objectClass) {
        try {
            Class.forName(objectClass.getName());
            return false;
        }
        catch (ClassNotFoundException e) {
            return true;
        }
    }

    /**
     * Appends spaces to end of input string for extending to needed length.
     *
     * @param s Input string.
     * @param targetLen Needed length.
     * @return String with appended spaces on the end.
     */
    public static String extendToLen(String s, int targetLen) {
        assert targetLen >= 0;
        assert s.length() <= targetLen;

        if (s.length() == targetLen)
            return s;

        SB sb = new SB(targetLen);

        sb.a(s);

        for (int i = 0; i < targetLen - s.length(); i++)
            sb.a(" ");

        return sb.toString();
    }

    /** */
    public static boolean isTxAwareQueriesEnabled(GridKernalContext kctx) {
        return kctx.config().getTransactionConfiguration().isTxAwareQueriesEnabled();
    }

    /** @return Empty binary context instance. */
    public static BinaryContext binaryContext(BinaryMarshaller marsh) {
        return binaryContext(BinaryUtils.cachingMetadataHandler(), marsh);
    }

    /** @return Empty binary context instance. */
    public static BinaryContext binaryContext(BinaryMetadataHandler metaHnd, BinaryMarshaller marsh) {
        return binaryContext(metaHnd, marsh, new IgniteConfiguration(), NullLogger.INSTANCE);
    }

    /** @return Empty binary context instance. */
    public static BinaryContext binaryContext(BinaryMarshaller marsh, IgniteConfiguration cfg) {
        return binaryContext(BinaryUtils.cachingMetadataHandler(), marsh, cfg, NullLogger.INSTANCE);
    }

    /** @return Empty binary context instance. */
    public static BinaryContext binaryContext(
        BinaryMetadataHandler metaHnd,
        BinaryMarshaller marsh,
        IgniteConfiguration cfg,
        IgniteLogger log
    ) {
        BinaryConfiguration bcfg = cfg.getBinaryConfiguration() == null ? new BinaryConfiguration() : cfg.getBinaryConfiguration();

        return useTestBinaryCtx
            ? new TestBinaryContext(
                metaHnd,
                marsh,
                cfg.getIgniteInstanceName(),
                cfg.getClassLoader(),
                bcfg.getSerializer(),
                bcfg.getIdMapper(),
                bcfg.getNameMapper(),
                bcfg.getTypeConfigurations(),
                CU.affinityFields(cfg),
                bcfg.isCompactFooter(),
                CU::affinityFieldName,
                log
            )
            : new BinaryContext(
                metaHnd,
                marsh,
                cfg.getIgniteInstanceName(),
                cfg.getClassLoader(),
                bcfg.getSerializer(),
                bcfg.getIdMapper(),
                bcfg.getNameMapper(),
                bcfg.getTypeConfigurations(),
                CU.affinityFields(cfg),
                bcfg.isCompactFooter(),
                CU::affinityFieldName,
                log
            );
    }

    /**
     * @param ctx {@link GridKernalContext}.
     * @return A warning message indicating excessive RAM usage or {@code null} if the RAM usage is within acceptable
     * limits.
     */
    @SuppressWarnings("ConstantConditions")
    public static String validateRamUsage(GridKernalContext ctx) {
        long ram = ctx.discovery().localNode().attribute(ATTR_PHY_RAM);

        if (ram != -1) {
            String macs = ctx.discovery().localNode().attribute(ATTR_MACS);

            long totalHeap = 0;
            long totalOffheap = 0;

            for (ClusterNode node : ctx.discovery().allNodes()) {
                if (macs.equals(node.attribute(ATTR_MACS))) {
                    long heap = node.metrics().getHeapMemoryMaximum();
                    Long offheap = node.<Long>attribute(ATTR_OFFHEAP_SIZE);

                    if (heap != -1)
                        totalHeap += heap;

                    if (offheap != null)
                        totalOffheap += offheap;
                }
            }

            long total = totalHeap + totalOffheap;

            if (total < 0)
                total = Long.MAX_VALUE;

            // 4GB or 20% of available memory is expected to be used by OS and user applications
            long safeToUse = ram - Math.max(4L << 30, (long)(ram * 0.2));

            if (total > safeToUse) {
                return "The total amount of RAM configured for nodes running on the local host exceeds " +
                    "the recommended maximum value. This may lead to significant slowdown due to swapping, or even " +
                    "JVM/Ignite crash with OutOfMemoryError (please decrease JVM heap size, data region size or " +
                    "checkpoint buffer size) [configured=" + (total >> 20) + "MB, available=" + (ram >> 20) +
                    "MB, recommended=" + (safeToUse >> 20) + "MB]";
            }
        }

        return null;
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class TestBinaryContext extends BinaryContext {
        /** */
        private List<TestBinaryContextListener> listeners;

        /** */
        public TestBinaryContext(
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
            super(
                metaHnd,
                marsh,
                igniteInstanceName,
                clsLdr,
                dfltSerializer,
                idMapper,
                nameMapper,
                typeCfgs,
                affFlds,
                compactFooter,
                affFldNameProvider,
                log
            );
        }


        /** {@inheritDoc} */
        @Nullable @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
            BinaryType metadata = super.metadata(typeId);

            if (listeners != null) {
                for (TestBinaryContextListener listener : listeners)
                    listener.onAfterMetadataRequest(typeId, metadata);
            }

            return metadata;
        }

        /** {@inheritDoc} */
        @Override public void updateMetadata(int typeId, BinaryMetadata meta, boolean failIfUnregistered) throws BinaryObjectException {
            if (listeners != null) {
                for (TestBinaryContextListener listener : listeners)
                    listener.onBeforeMetadataUpdate(typeId, meta);
            }

            super.updateMetadata(typeId, meta, failIfUnregistered);
        }

        /** */
        public interface TestBinaryContextListener {
            /**
             * @param typeId Type id.
             * @param type Type.
             */
            void onAfterMetadataRequest(int typeId, BinaryType type);

            /**
             * @param typeId Type id.
             * @param metadata Metadata.
             */
            void onBeforeMetadataUpdate(int typeId, BinaryMetadata metadata);
        }

        /** @param lsnr Listener. */
        public void addListener(TestBinaryContextListener lsnr) {
            if (listeners == null)
                listeners = new ArrayList<>();

            if (!listeners.contains(lsnr))
                listeners.add(lsnr);
        }

        /** */
        public void clearAllListener() {
            if (listeners != null)
                listeners.clear();
        }
    }
}
