/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcRuntimeException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TimeUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.concurrent.Future;
import scala.reflect.ClassTag$;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive RPC
 * invocations from a {@link RpcGateway}.
 */

@ThreadSafe
public class AkkaRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

    static final int VERSION = 2;

    private final Object lock = new Object();

    private final ActorSystem actorSystem;
    private final AkkaRpcServiceConfiguration configuration;

    //AkkaRpcService是RpcService的唯一实现类。
    //持有AkkaSystem的引用
    //还维护所有注册了的RpcEndpoint的引用，为每个RpcEndpoint分配一个ActorRef并保存它们的对应关系。（实际上每个RE不就对应一个actor吗？）
    @GuardedBy("lock")
    private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

    private final String address;
    private final int port;

    private final boolean captureAskCallstacks;

    private final ScheduledExecutor internalScheduledExecutor;

    private final CompletableFuture<Void> terminationFuture;

    private final Supervisor supervisor;

    private volatile boolean stopped;

    //此构造方法要传入actorSystem和关于rpcService相关配置
    @VisibleForTesting
    public AkkaRpcService(
            final ActorSystem actorSystem, final AkkaRpcServiceConfiguration configuration) {
        this.actorSystem = checkNotNull(actorSystem, "actor system");
        this.configuration = checkNotNull(configuration, "akka rpc service configuration");

        //获取ActorSystem的host地址。（这个address是包含了类似于akka.tcp://main3@127.0.0.1:2553这样的结构。）
        Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

        //获取ActorSystem的host地址
        if (actorSystemAddress.host().isDefined()) {
            address = actorSystemAddress.host().get();
        } else {
            address = "";
        }

        //获取端口号，如果没有配置，返回-1
        if (actorSystemAddress.port().isDefined()) {
            port = (Integer) actorSystemAddress.port().get();
        } else {
            port = -1;
        }

        //是否捕捉ask操作的调用栈。（就是actor的ask方法的调用栈？）
        captureAskCallstacks = configuration.captureAskCallStack();

        //ActorSystem scheduler的一个包装类。（是否每一个AcorSystem中，都会有一个Scheduler呢？或者是Flink里面定义的）
        internalScheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem);

        //RPC服务停机之后的异步回调
        terminationFuture = new CompletableFuture<>();

        //标记RPC服务的状态，当前是正在运行。（因此stopped是false）
        stopped = false;
        //启动supervisorActor,（这个是否是最顶层的？supervisorActor应当是每个ActorSystem都有一个吧，应当是作为最顶层的Actor？）
        //一个AkkaRpcService可被多个RpcEndpoint使用，为每个RpcEndpoint创建一个Actor。
        //这些Actor都是supervisorActor的子Actor
        //（这个supervisor是flink中规定的？如果不是，难道不应当是同ActorSystem一块启动吗？）
        supervisor = startSupervisorActor();
    }

    private Supervisor startSupervisorActor() {
        //竟然定义了一个单个线程的线程池
        final ExecutorService terminationFutureExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "AkkaRpcService-Supervisor-Termination-Future-Executor"));
        //创建一个SupervisorActor
        final ActorRef actorRef =
                SupervisorActor.startSupervisorActor(actorSystem, terminationFutureExecutor);

        //将SupervisorActor和对应的线程池包装一块作为Supervisor返回
        return Supervisor.create(actorRef, terminationFutureExecutor);
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    protected int getVersion() {
        return VERSION;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    // this method does not mutate state and is thus thread-safe
    //此方法可以连接到远程RpcService。
    //1参是给定地址，2参是和远程通信的途径（出入口、途径、方法。这里应当是指要调用的功能？）。
    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(
            final String address, final Class<C> clazz) {

        return connectInternal(
                address,
                clazz,
                //3参是一个工厂方法
                //从actorRef创建出InvocationHandler
                //这个方法会在connectInternal中使用
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new AkkaInvocationHandler(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            null,
                            captureAskCallstacks);
                });
    }

    // this method does not mutate state and is thus thread-safe
    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new FencedAkkaInvocationHandler<>(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks);
                });
    }

    @Override
    //starServer方法，在创建RpcEndpoint的时候调用。（在RpcEndpoint的构造方法中会调用此方法）
    //指定泛型C，是RpcEndpoint和RpcGateway的共同子类型,但是RpcEndpoint本身就实现了RpcGateway，为什么还需这样写
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        checkNotNull(rpcEndpoint, "rpc endpoint");

        //向SupervisorActor注册，生成一个新的Actor。
        //每个rpcEndpoint都有一个对应一个Actor？
        final SupervisorActor.ActorRegistration actorRegistration =
                registerAkkaRpcActor(rpcEndpoint);
        //
        final ActorRef actorRef = actorRegistration.getActorRef();
        final CompletableFuture<Void> actorTerminationFuture =
                actorRegistration.getTerminationFuture();

        LOG.info(
                "Starting RPC endpoint for {} at {} .",
                rpcEndpoint.getClass().getName(),
                actorRef.path());

        final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        Set<Class<?>> implementedRpcGateways =
                new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

        implementedRpcGateways.add(RpcServer.class);
        implementedRpcGateways.add(AkkaBasedEndpoint.class);

        final InvocationHandler akkaInvocationHandler;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            // a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
            akkaInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            actorTerminationFuture,
                            ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                            captureAskCallstacks);

            implementedRpcGateways.add(FencedMainThreadExecutable.class);
        } else {
            akkaInvocationHandler =
                    new AkkaInvocationHandler(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            actorTerminationFuture,
                            captureAskCallstacks);
        }

        // Rather than using the System ClassLoader directly, we derive the ClassLoader
        // from this class . That works better in cases where Flink runs embedded and all Flink
        // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
        ClassLoader classLoader = getClass().getClassLoader();

        @SuppressWarnings("unchecked")
        RpcServer server =
                (RpcServer)
                        Proxy.newProxyInstance(
                                classLoader,
                                implementedRpcGateways.toArray(
                                        new Class<?>[implementedRpcGateways.size()]),
                                akkaInvocationHandler);

        return server;
    }

    //starServer中，需要向SupervisorActor注册，生成一个新的Actor
    private <C extends RpcEndpoint & RpcGateway>
            SupervisorActor.ActorRegistration registerAkkaRpcActor(C rpcEndpoint) {
        final Class<? extends AbstractActor> akkaRpcActorType;

        //判断RpcEndpoint的类型
        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            akkaRpcActorType = FencedAkkaRpcActor.class;
        } else {
            akkaRpcActorType = AkkaRpcActor.class;
        }

        //（为什么要加锁？）
        synchronized (lock) {
            //先检查RpcService状态，需要不是stopped
            checkState(!stopped, "RpcService is stopped");

            //从SupervisorActor创建一个新的Actor（返回的是创建时的相应消息）
            final SupervisorActor.StartAkkaRpcActorResponse startAkkaRpcActorResponse =
                    //（这里需要supervisor、相应actor的工厂方法、endpointId）
                    SupervisorActor.startAkkaRpcActor(
                            //获取SupervisorActor
                            supervisor.getActor(),
                            //传入Actor构造工厂方法（记得，是一个方法propsFactory）
                            actorTerminationFuture ->
                                    Props.create(
                                            akkaRpcActorType,
                                            rpcEndpoint,
                                            actorTerminationFuture,
                                            getVersion(),
                                            configuration.getMaximumFramesize()),
                            //还需要endpoint的id
                            rpcEndpoint.getEndpointId());

            //为actorRegistration绑定异常响应
            final SupervisorActor.ActorRegistration actorRegistration =
                    //这个orElseThrow是指如果，调用者是空，则抛出异常
                    startAkkaRpcActorResponse.orElseThrow(
                            cause ->
                                    new AkkaRpcRuntimeException(
                                            String.format(
                                                    "Could not create the %s for %s.",
                                                    AkkaRpcActor.class.getSimpleName(),
                                                    rpcEndpoint.getEndpointId()),
                                            cause));

            //将这个新创建的actor和对应的rpcEndpoint对应关系保存
            actors.put(actorRegistration.getActorRef(), rpcEndpoint);

            return actorRegistration;
        }
    }

    @Override
    public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
        if (rpcServer instanceof AkkaBasedEndpoint) {

            InvocationHandler fencedInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            rpcServer.getAddress(),
                            rpcServer.getHostname(),
                            ((AkkaBasedEndpoint) rpcServer).getActorRef(),
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks);

            // Rather than using the System ClassLoader directly, we derive the ClassLoader
            // from this class . That works better in cases where Flink runs embedded and all Flink
            // code is loaded dynamically (for example from an OSGI bundle) through a custom
            // ClassLoader
            ClassLoader classLoader = getClass().getClassLoader();

            return (RpcServer)
                    Proxy.newProxyInstance(
                            classLoader,
                            new Class<?>[] {RpcServer.class, AkkaBasedEndpoint.class},
                            fencedInvocationHandler);
        } else {
            throw new RuntimeException(
                    "The given RpcServer must implement the AkkaGateway in order to fence it.");
        }
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        if (selfGateway instanceof AkkaBasedEndpoint) {
            final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
            final RpcEndpoint rpcEndpoint;

            synchronized (lock) {
                if (stopped) {
                    return;
                } else {
                    rpcEndpoint = actors.remove(akkaClient.getActorRef());
                }
            }

            if (rpcEndpoint != null) {
                terminateAkkaRpcActor(akkaClient.getActorRef(), rpcEndpoint);
            } else {
                LOG.debug(
                        "RPC endpoint {} already stopped or from different RPC service",
                        selfGateway.getAddress());
            }
        }
    }

    @Override
    public CompletableFuture<Void> stopService() {
        final CompletableFuture<Void> akkaRpcActorsTerminationFuture;

        synchronized (lock) {
            if (stopped) {
                return terminationFuture;
            }

            LOG.info("Stopping Akka RPC service.");

            stopped = true;

            akkaRpcActorsTerminationFuture = terminateAkkaRpcActors();
        }

        final CompletableFuture<Void> supervisorTerminationFuture =
                FutureUtils.composeAfterwards(
                        akkaRpcActorsTerminationFuture, supervisor::closeAsync);

        final CompletableFuture<Void> actorSystemTerminationFuture =
                FutureUtils.composeAfterwards(
                        supervisorTerminationFuture,
                        () -> FutureUtils.toJava(actorSystem.terminate()));

        actorSystemTerminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        terminationFuture.completeExceptionally(throwable);
                    } else {
                        terminationFuture.complete(null);
                    }

                    LOG.info("Stopped Akka RPC service.");
                });

        return terminationFuture;
    }

    @GuardedBy("lock")
    @Nonnull
    private CompletableFuture<Void> terminateAkkaRpcActors() {
        final Collection<CompletableFuture<Void>> akkaRpcActorTerminationFutures =
                new ArrayList<>(actors.size());

        for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
            akkaRpcActorTerminationFutures.add(
                    terminateAkkaRpcActor(
                            actorRefRpcEndpointEntry.getKey(),
                            actorRefRpcEndpointEntry.getValue()));
        }
        actors.clear();

        return FutureUtils.waitForAll(akkaRpcActorTerminationFutures);
    }

    private CompletableFuture<Void> terminateAkkaRpcActor(
            ActorRef akkaRpcActorRef, RpcEndpoint rpcEndpoint) {
        akkaRpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

        return rpcEndpoint.getTerminationFuture();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public Executor getExecutor() {
        return actorSystem.dispatcher();
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return internalScheduledExecutor;
    }

    @Override
    public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
        checkNotNull(runnable, "runnable");
        checkNotNull(unit, "unit");
        checkArgument(delay >= 0L, "delay must be zero or larger");

        return internalScheduledExecutor.schedule(runnable, delay, unit);
    }

    @Override
    public void execute(Runnable runnable) {
        actorSystem.dispatcher().execute(runnable);
    }

    @Override
    public <T> CompletableFuture<T> execute(Callable<T> callable) {
        Future<T> scalaFuture = Futures.<T>future(callable, actorSystem.dispatcher());

        return FutureUtils.toJava(scalaFuture);
    }

    // ---------------------------------------------------------------------------------------
    // Private helper methods
    // ---------------------------------------------------------------------------------------

    private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
        final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        return Tuple2.of(actorAddress, hostname);
    }

    private <C extends RpcGateway> CompletableFuture<C> connectInternal(
            final String address,
            final Class<C> clazz,
            Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
        checkState(!stopped, "RpcService is stopped");

        LOG.debug(
                "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
                address,
                clazz.getName());

        //从远程地址获取ActorRef，并且有超时时间的，具体可以进去看方法
        final CompletableFuture<ActorRef> actorRefFuture = resolveActorAddress(address);

        //获取到ActorRef后执行
        final CompletableFuture<HandshakeSuccessMessage> handshakeFuture =
                //通过前面获取ActorRef后做的
                actorRefFuture.thenCompose(
                        (ActorRef actorRef) ->
                                FutureUtils.toJava(
                                        //发送RemoteHandshakeMessage给远端
                                        //这是握手消息
                                        Patterns.ask(
                                                        actorRef,
                                                        new RemoteHandshakeMessage(
                                                                clazz, getVersion()),
                                                        configuration.getTimeout().toMilliseconds())
                                                //握手成功后的消息，还需要map转成什么？
                                                .<HandshakeSuccessMessage>mapTo(
                                                        ClassTag$.MODULE$
                                                                .<HandshakeSuccessMessage>apply(
                                                                        HandshakeSuccessMessage
                                                                                .class))));

        //返回一个创建RpcGateway的CompletableFuture（拿到握手成功的消息，可以返回）
        return actorRefFuture.thenCombineAsync(
                handshakeFuture,
                //这个二参的方法，就是在握手操作完毕后执行的
                (ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
                    //使用前面方法传入的invocationHandlerFactory,使用actorRef创建出一个AkkaInvocationHandler
                    InvocationHandler invocationHandler = invocationHandlerFactory.apply(actorRef);

                    // Rather than using the System ClassLoader directly, we derive the ClassLoader
                    // from this class . That works better in cases where Flink runs embedded and
                    // all Flink
                    // code is loaded dynamically (for example from an OSGI bundle) through a custom
                    // ClassLoader
                    ClassLoader classLoader = getClass().getClassLoader();

                    //创建出代理类，类型转化为C类型（应当就是一个actor的某个功能？强转后就只剩实现一个gateway的方法？）
                    @SuppressWarnings("unchecked")
                    C proxy =
                            (C)
                                    Proxy.newProxyInstance(
                                            classLoader, new Class<?>[] {clazz}, invocationHandler);

                    return proxy;
                },
                //这个应当是线程池？
                actorSystem.dispatcher());
    }

    private CompletableFuture<ActorRef> resolveActorAddress(String address) {
        final ActorSelection actorSel = actorSystem.actorSelection(address);

        return actorSel.resolveOne(TimeUtils.toDuration(configuration.getTimeout()))
                .toCompletableFuture()
                .exceptionally(
                        error -> {
                            throw new CompletionException(
                                    new RpcConnectionException(
                                            String.format(
                                                    "Could not connect to rpc endpoint under address %s.",
                                                    address),
                                            error));
                        });
    }

    // ---------------------------------------------------------------------------------------
    // Private inner classes
    // ---------------------------------------------------------------------------------------

    private static final class Supervisor implements AutoCloseableAsync {

        private final ActorRef actor;

        private final ExecutorService terminationFutureExecutor;

        private Supervisor(ActorRef actor, ExecutorService terminationFutureExecutor) {
            this.actor = actor;
            this.terminationFutureExecutor = terminationFutureExecutor;
        }

        private static Supervisor create(
                ActorRef actorRef, ExecutorService terminationFutureExecutor) {
            return new Supervisor(actorRef, terminationFutureExecutor);
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return ExecutorUtils.nonBlockingShutdown(
                    30L, TimeUnit.SECONDS, terminationFutureExecutor);
        }
    }
}
