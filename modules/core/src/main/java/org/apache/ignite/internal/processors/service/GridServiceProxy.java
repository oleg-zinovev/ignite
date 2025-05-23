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

package org.apache.ignite.internal.processors.service;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridClosureCallMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.platform.PlatformServiceMethod;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceCallInterceptor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;

/**
 * Wrapper for making {@link org.apache.ignite.services.Service} class proxies.
 */
public class GridServiceProxy<T> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Method PLATFORM_SERVICE_INVOKE_METHOD;

    static {
        try {
            PLATFORM_SERVICE_INVOKE_METHOD = PlatformService.class.getMethod("invokeMethod", String.class,
                    boolean.class, boolean.class, Object[].class, Map.class);
        }
        catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError("'invokeMethod' is not defined in " + PlatformService.class.getName());
        }
    }

    /** Grid logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Proxy object. */
    private final T proxy;

    /** Grid projection. */
    private final ClusterGroup prj;

    /** Kernal context. */
    @GridToStringExclude
    private final GridKernalContext ctx;

    /** Remote node to use for proxy invocation. */
    private final AtomicReference<ClusterNode> rmtNode = new AtomicReference<>();

    /** {@code True} if projection includes local node. */
    private boolean hasLocNode;

    /** Service name. */
    private final String name;

    /** Whether multi-node request should be done. */
    private final boolean sticky;

    /** Service availability wait timeout. */
    private final long waitTimeout;

    /** */
    private final boolean keepBinary;

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param svc Service type class.
     * @param sticky Whether multi-node request should be done.
     * @param timeout Service availability wait timeout. Cannot be negative.
     * @param ctx Context.
     * @param callAttrsProvider Service call context attributes provider.
     * @param keepBinary {@code True} if results should be in binary form.
     */
    public GridServiceProxy(ClusterGroup prj,
        String name,
        Class<? super T> svc,
        boolean sticky,
        long timeout,
        GridKernalContext ctx,
        @Nullable Supplier<Map<String, Object>> callAttrsProvider,
        boolean keepBinary
    ) {
        assert timeout >= 0 : timeout;

        this.prj = prj;
        this.ctx = ctx;
        this.name = name;
        this.sticky = sticky;
        this.keepBinary = keepBinary;

        waitTimeout = timeout;
        hasLocNode = hasLocalNode(prj);

        log = ctx.log(getClass());

        proxy = (T)Proxy.newProxyInstance(
            svc.getClassLoader(),
            new Class[] {svc},
            new ProxyInvocationHandler(callAttrsProvider)
        );
    }

    /**
     * @param prj Grid nodes projection.
     * @return Whether given projection contains any local node.
     */
    private boolean hasLocalNode(ClusterGroup prj) {
        for (ClusterNode n : prj.nodes()) {
            if (n.isLocal())
                return true;
        }

        return false;
    }

    /**
     * Invoke the method.
     *
     * @param mtd Method.
     * @param args Arugments.
     * @param callAttrs Service call context attributes.
     * @return Result.
     */
    @SuppressWarnings("BusyWait")
    public Object invokeMethod(Method mtd, Object[] args, @Nullable Map<String, Object> callAttrs) throws Throwable {
        if (U.isHashCodeMethod(mtd))
            return System.identityHashCode(proxy);
        else if (U.isEqualsMethod(mtd))
            return proxy == args[0];
        else if (U.isToStringMethod(mtd))
            return GridServiceProxy.class.getSimpleName() + " [name=" + name + ", sticky=" + sticky + ']';

        ctx.gateway().readLock();

        try {
            final long startTime = U.currentTimeMillis();

            while (true) {
                ClusterNode node = null;

                try {
                    node = nodeForService(name, sticky);

                    if (node == null)
                        throw new IgniteException("Failed to find deployed service: " + name);

                    // If service is deployed locally, then execute locally.
                    if (node.isLocal()) {
                        ServiceContextImpl svcCtx = ctx.service().serviceContext(name);

                        if (svcCtx != null) {
                            Service svc = svcCtx.service();

                            if (svc != null) {
                                HistogramMetricImpl hist = svcCtx.isStatisticsEnabled() ?
                                    invocationHistogramm(svcCtx, mtd.getName(), args) : null;

                                return hist == null ? callServiceLocally(svcCtx, mtd, args, callAttrs) :
                                    measureCall(hist, () -> callServiceLocally(svcCtx, mtd, args, callAttrs));
                            }
                        }
                    }
                    else {
                        // Execute service remotely.
                        return unmarshalResult(ctx.closure().callAsync(
                                GridClosureCallMode.BROADCAST,
                                new ServiceProxyCallable(methodName(mtd), name, mtd.getParameterTypes(), args, callAttrs),
                                options(Collections.singleton(node))
                                    .withPool(SERVICE_POOL)
                                    .withFailoverDisabled()
                                    .withTimeout(waitTimeout)
                            ).get());
                    }
                }
                catch (InvocationTargetException e) {
                    // For local services rethrow original exception.
                    throw e.getTargetException();
                }
                catch (RuntimeException | Error e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    // Check if ignorable exceptions are in the cause chain.
                    Throwable ignorableCause = X.cause(e, ClusterTopologyCheckedException.class);

                    if (ignorableCause == null)
                        ignorableCause = X.cause(e, GridServiceNotFoundException.class);

                    if (ignorableCause != null) {
                        if (log.isDebugEnabled())
                            log.debug("Service was not found or topology changed (will retry): " + ignorableCause.getMessage());
                    }
                    else {
                        // Rethrow original service method exception so that calling user code can handle it correctly.
                        ServiceProxyException svcProxyE = X.cause(e, ServiceProxyException.class);

                        if (svcProxyE != null)
                            throw svcProxyE.getCause();

                        throw U.convertException(e);
                    }
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }

                // If we are here, that means that service was not found
                // or topology was changed. In this case, we erase the
                // previous sticky node and try again.
                rmtNode.compareAndSet(node, null);

                // Add sleep between retries to avoid busy-wait loops.
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }

                if (waitTimeout > 0 && U.currentTimeMillis() - startTime >= waitTimeout)
                    throw new IgniteException("Service acquire timeout was reached, stopping. [timeout=" + waitTimeout + "]");
            }
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /**
     * @param svcCtx Service context.
     * @param mtd Method to call.
     * @param args Method args.
     * @param callAttrs Service call context attributes.
     * @return Invocation result.
     */
    private Object callServiceLocally(
        ServiceContextImpl svcCtx,
        Method mtd,
        Object[] args,
        @Nullable Map<String, Object> callAttrs
    ) throws Exception {
        Service svc = svcCtx.service();

        if (svc instanceof PlatformService && !PLATFORM_SERVICE_INVOKE_METHOD.equals(mtd))
            return ((PlatformService)svc).invokeMethod(methodName(mtd), false, true, args, callAttrs);
        else
            return callServiceMethod(svcCtx, mtd, args, callAttrs);
    }

    /**
     * @param svcCtx Service context.
     * @param mtd Method to call.
     * @param args Method args.
     * @param callAttrs Service call context attributes.
     * @return Invocation result.
     */
    private static Object callServiceMethod(
        ServiceContextImpl svcCtx,
        Method mtd,
        Object[] args,
        @Nullable Map<String, Object> callAttrs
    ) throws Exception {
        ServiceCallContextImpl prevCtx = null;

        if (callAttrs != null) {
            // One service can be called from another in the same thread.
            prevCtx = ServiceCallContextHolder.current();

            ServiceCallContextHolder.current(new ServiceCallContextImpl(callAttrs));
        }

        try {
            ServiceCallInterceptor interceptor = svcCtx.interceptor();

            return interceptor == null ?
                mtd.invoke(svcCtx.service(), args) :
                interceptor.invoke(mtd.getName(), args, svcCtx, () -> mtd.invoke(svcCtx.service(), args));
        }
        finally {
            if (callAttrs != null)
                ServiceCallContextHolder.current(prevCtx);
        }
    }

    /** */
    private Object unmarshalResult(byte[] res) throws IgniteCheckedException {
        BinaryMarshaller marsh = ctx.marshaller();

        if (keepBinary && BinaryUtils.useBinaryArrays()) {
            // To avoid deserializing of enum types and BinaryArrays.
            return marsh.binaryMarshaller().unmarshal(res, null);
        }
        else
            return U.unmarshal(marsh, res, null);
    }

    /**
     * @param sticky Whether multi-node request should be done.
     * @param name Service name.
     * @return Node with deployed service or {@code null} if there is no such node.
     */
    private ClusterNode nodeForService(String name, boolean sticky) throws IgniteCheckedException {
        do { // Repeat if reference to remote node was changed.
            if (sticky) {
                ClusterNode curNode = rmtNode.get();

                if (curNode != null)
                    return curNode;

                curNode = randomNodeForService(name);

                if (curNode == null)
                    return null;

                if (rmtNode.compareAndSet(null, curNode))
                    return curNode;
            }
            else
                return randomNodeForService(name);
        }
        while (true);
    }

    /**
     * @param name Service name.
     * @return Local node if it has a given service deployed or randomly chosen remote node,
     * otherwise ({@code null} if given service is not deployed on any node.
     */
    private ClusterNode randomNodeForService(String name) throws IgniteCheckedException {
        if (hasLocNode && ctx.service().service(name) != null)
            return ctx.discovery().localNode();

        Map<UUID, Integer> snapshot = ctx.service().serviceTopology(name, waitTimeout);

        if (snapshot == null || snapshot.isEmpty())
            return null;

        // Optimization for cluster singletons.
        if (snapshot.size() == 1) {
            UUID nodeId = snapshot.keySet().iterator().next();

            return prj.node(nodeId);
        }

        Collection<ClusterNode> nodes = prj.nodes();

        // Optimization for 1 node in projection.
        if (nodes.size() == 1) {
            ClusterNode n = nodes.iterator().next();

            return snapshot.containsKey(n.id()) ? n : null;
        }

        // Optimization if projection is the whole grid.
        if (prj.predicate() == F.<ClusterNode>alwaysTrue()) {
            int idx = ThreadLocalRandom.current().nextInt(snapshot.size());

            int i = 0;

            // Get random node.
            for (Map.Entry<UUID, Integer> e : snapshot.entrySet()) {
                if (i++ >= idx) {
                    if (e.getValue() > 0)
                        return ctx.discovery().node(e.getKey());
                }
            }

            i = 0;

            // Circle back.
            for (Map.Entry<UUID, Integer> e : snapshot.entrySet()) {
                if (e.getValue() > 0)
                    return ctx.discovery().node(e.getKey());

                if (i++ == idx)
                    return null;
            }
        }
        else {
            List<ClusterNode> nodeList = new ArrayList<>(nodes.size());

            for (ClusterNode n : nodes) {
                Integer cnt = snapshot.get(n.id());

                if (cnt != null && cnt > 0)
                    nodeList.add(n);
            }

            if (nodeList.isEmpty())
                return null;

            int idx = ThreadLocalRandom.current().nextInt(nodeList.size());

            return nodeList.get(idx);
        }

        return null;
    }

    /**
     * @return Proxy object for a given instance.
     */
    T proxy() {
        return proxy;
    }

    /**
     * @param mtd Method to invoke.
     */
    private static String methodName(Method mtd) {
        PlatformServiceMethod ann = mtd.getDeclaredAnnotation(PlatformServiceMethod.class);

        return ann == null ? mtd.getName() : ann.value();
    }

    /**
     * Calls the target, measures and registers its duration.
     *
     * @param histogram Related metric.
     * @param target    Target to call and measure.
     */
    private static <T> T measureCall(
            HistogramMetricImpl histogram,
            Callable<T> target
    ) throws Exception {
        long startTime = System.nanoTime();

        try {
            return target.call();
        }
        finally {
            histogram.value(System.nanoTime() - startTime);
        }
    }

    /**
     * Invocation handler for service proxy.
     */
    private class ProxyInvocationHandler implements InvocationHandler {
        /** Service call context attributes provider. */
        private final Supplier<Map<String, Object>> callAttrsProvider;

        /**
         * @param callAttrsProvider Service call context attributes provider.
         */
        public ProxyInvocationHandler(@Nullable Supplier<Map<String, Object>> callAttrsProvider) {
            this.callAttrsProvider = callAttrsProvider;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, final Method mtd, final Object[] args) throws Throwable {
            return invokeMethod(mtd, args, callAttrsProvider != null ? callAttrsProvider.get() : null);
        }
    }

    /**
     * Callable proxy class.
     */
    private static class ServiceProxyCallable implements IgniteCallable<byte[]>, Externalizable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Method name. */
        private String mtdName;

        /** Service name. */
        private String svcName;

        /** Argument types. */
        private Class<?>[] argTypes;

        /** Args. */
        private Object[] args;

        /** Service call context attributes. */
        private Map<String, Object> callAttrs;

        /** Grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ServiceProxyCallable() {
            // No-op.
        }

        /**
         * @param mtdName Service method to invoke.
         * @param svcName Service name.
         * @param argTypes Argument types.
         * @param args Arguments for invocation.
         * @param callAttrs Service call context attributes.
         */
        private ServiceProxyCallable(
            String mtdName,
            String svcName,
            Class<?>[] argTypes,
            Object[] args,
            @Nullable Map<String, Object> callAttrs
        ) {
            this.mtdName = mtdName;
            this.svcName = svcName;
            this.argTypes = argTypes;
            this.args = args;
            this.callAttrs = callAttrs;
        }

        /** {@inheritDoc} */
        @Override public byte[] call() throws Exception {
            ServiceContextImpl ctx = ignite.context().service().serviceContext(svcName);

            if (ctx == null || ctx.service() == null)
                throw new GridServiceNotFoundException(svcName);

            GridServiceMethodReflectKey key = new GridServiceMethodReflectKey(mtdName, argTypes);

            Method mtd = ctx.method(key);

            HistogramMetricImpl hist = ctx.isStatisticsEnabled() ? invocationHistogramm(ctx, mtdName, args) : null;

            Object res = hist == null ? callService(ctx, mtd) : measureCall(hist, () -> callService(ctx, mtd));

            return U.marshal(ignite.context().marshaller(), res);
        }

        /** */
        private Object callService(ServiceContextImpl svcCtx, Method mtd) throws Exception {
            if (svcCtx.service() instanceof PlatformService && mtd == null)
                return callPlatformService((PlatformService)svcCtx.service());
            else
                return callOrdinaryService(svcCtx, mtd);
        }

        /** */
        private Object callPlatformService(PlatformService srv) {
            try {
                return srv.invokeMethod(mtdName, false, true, args, callAttrs);
            }
            catch (PlatformNativeException ne) {
                throw new ServiceProxyException(U.convertException(ne));
            }
            catch (Exception e) {
                throw new ServiceProxyException(e);
            }
        }

        /** */
        private Object callOrdinaryService(ServiceContextImpl svcCtx, Method mtd) throws Exception {
            if (mtd == null)
                throw new GridServiceMethodNotFoundException(svcName, mtdName, argTypes);

            try {
                return callServiceMethod(svcCtx, mtd, args, callAttrs);
            }
            catch (InvocationTargetException e) {
                throw new ServiceProxyException(e.getCause());
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, svcName);
            U.writeString(out, mtdName);
            U.writeArray(out, argTypes);
            U.writeArray(out, args);
            U.writeMap(out, callAttrs);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            svcName = U.readString(in);
            mtdName = U.readString(in);
            argTypes = U.readClassArray(in);
            args = U.readArray(in);
            callAttrs = U.readMap(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ServiceProxyCallable.class, this);
        }
    }

    /**
     * @return Invocation histogramm for the method.
     */
    private static HistogramMetricImpl invocationHistogramm(ServiceContextImpl ctx, String mtdName, Object[] args) {
        if (ctx.service() instanceof PlatformService) {
            assert args.length > 0 && args[0] instanceof String;
            assert ctx.metrics() != null;

            return ctx.metrics().findMetric((String)args[0]);
        }
        else
            return ctx.metrics().findMetric(mtdName);
    }

    /**
     * Exception class that wraps an exception thrown by the service implementation.
     */
    private static class ServiceProxyException extends RuntimeException {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        ServiceProxyException(Throwable cause) {
            super(cause);
        }
    }
}
