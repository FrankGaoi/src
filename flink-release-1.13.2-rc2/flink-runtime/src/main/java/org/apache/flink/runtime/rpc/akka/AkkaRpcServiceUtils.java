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
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service, or
 * constructing RPC addresses.
 * 此类中含有许多关于rpc使用的辅助方法，例如启动rpcService、构造rpc地址等等
 */
public class AkkaRpcServiceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcServiceUtils.class);

    private static final String AKKA_TCP = "akka.tcp";
    private static final String AKKA_SSL_TCP = "akka.ssl.tcp";

    static final String SUPERVISOR_NAME = "rpc";

    private static final String SIMPLE_AKKA_CONFIG_TEMPLATE =
            "akka {remote {netty.tcp {maximum-frame-size = %s}}}";

    private static final String MAXIMUM_FRAME_SIZE_PATH =
            "akka.remote.netty.tcp.maximum-frame-size";

    private static final AtomicLong nextNameOffset = new AtomicLong(0L);

    // ------------------------------------------------------------------------
    //  RPC instantiation
    // rpc实例化（疑惑在于，为什么这里创建rpcService有remote。
    // ------------------------------------------------------------------------

    public static AkkaRpcService createRemoteRpcService(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        //创建一个serviceBuilder
        final AkkaRpcServiceBuilder akkaRpcServiceBuilder =
                AkkaRpcServiceUtils.remoteServiceBuilder(
                        configuration, externalAddress, externalPortRange);

        //传入bind地址和bind端口号配置
        if (bindAddress != null) {
            akkaRpcServiceBuilder.withBindAddress(bindAddress);
        }

        bindPort.ifPresent(akkaRpcServiceBuilder::withBindPort);

        //通过builder创建并启动一个rpcService
        return akkaRpcServiceBuilder.createAndStart();
    }
    //可见此方法会返回一个rpcServiceBuilder，此对象专门由于建RpcService
    //参数竟然是只要外部地址和外部端口范围
    public static AkkaRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return new AkkaRpcServiceBuilder(configuration, LOG, externalAddress, externalPortRange);
    }

    @VisibleForTesting
    public static AkkaRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration, @Nullable String externalAddress, int externalPort) {
        return remoteServiceBuilder(configuration, externalAddress, String.valueOf(externalPort));
    }

    public static AkkaRpcServiceBuilder localServiceBuilder(Configuration configuration) {
        return new AkkaRpcServiceBuilder(configuration, LOG);
    }

    // ------------------------------------------------------------------------
    //  RPC endpoint addressing
    // ------------------------------------------------------------------------

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param config The configuration from which to deduce further settings.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            HighAvailabilityServicesUtils.AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {

        checkNotNull(config, "config is null");

        final boolean sslEnabled =
                config.getBoolean(AkkaOptions.SSL_ENABLED) && SSLUtils.isInternalSSLEnabled(config);

        return getRpcUrl(
                hostname,
                port,
                endpointName,
                addressResolution,
                sslEnabled ? AkkaProtocol.SSL_TCP : AkkaProtocol.TCP);
    }

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param akkaProtocol True, if security/encryption is enabled, false otherwise.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            HighAvailabilityServicesUtils.AddressResolution addressResolution,
            AkkaProtocol akkaProtocol)
            throws UnknownHostException {

        checkNotNull(hostname, "hostname is null");
        checkNotNull(endpointName, "endpointName is null");
        checkArgument(isValidClientPort(port), "port must be in [1, 65535]");

        if (addressResolution == AddressResolution.TRY_ADDRESS_RESOLUTION) {
            // Fail fast if the hostname cannot be resolved
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getByName(hostname);
        }

        final String hostPort = NetUtils.unresolvedHostAndPortToNormalizedString(hostname, port);

        return internalRpcUrl(
                endpointName, Optional.of(new RemoteAddressInformation(hostPort, akkaProtocol)));
    }

    public static String getLocalRpcUrl(String endpointName) {
        return internalRpcUrl(endpointName, Optional.empty());
    }

    private static final class RemoteAddressInformation {
        private final String hostnameAndPort;
        private final AkkaProtocol akkaProtocol;

        private RemoteAddressInformation(String hostnameAndPort, AkkaProtocol akkaProtocol) {
            this.hostnameAndPort = hostnameAndPort;
            this.akkaProtocol = akkaProtocol;
        }

        private String getHostnameAndPort() {
            return hostnameAndPort;
        }

        private AkkaProtocol getAkkaProtocol() {
            return akkaProtocol;
        }
    }

    private static String internalRpcUrl(
            String endpointName, Optional<RemoteAddressInformation> remoteAddressInformation) {
        final String protocolPrefix =
                remoteAddressInformation
                        .map(rai -> akkaProtocolToString(rai.getAkkaProtocol()))
                        .orElse("akka");
        final Optional<String> optionalHostnameAndPort =
                remoteAddressInformation.map(RemoteAddressInformation::getHostnameAndPort);

        final StringBuilder url = new StringBuilder(String.format("%s://flink", protocolPrefix));
        optionalHostnameAndPort.ifPresent(hostPort -> url.append("@").append(hostPort));

        url.append("/user/").append(SUPERVISOR_NAME).append("/").append(endpointName);

        // protocolPrefix://flink[@hostname:port]/user/rpc/endpointName
        return url.toString();
    }

    private static String akkaProtocolToString(AkkaProtocol akkaProtocol) {
        return akkaProtocol == AkkaProtocol.SSL_TCP ? AKKA_SSL_TCP : AKKA_TCP;
    }

    /** Whether to use TCP or encrypted TCP for Akka. */
    public enum AkkaProtocol {
        TCP,
        SSL_TCP
    }

    /**
     * Creates a random name of the form prefix_X, where X is an increasing number.
     *
     * @param prefix Prefix string to prepend to the monotonically increasing name offset number
     * @return A random name of the form prefix_X where X is an increasing number
     */
    public static String createRandomName(String prefix) {
        Preconditions.checkNotNull(prefix, "Prefix must not be null.");

        long nameOffset;

        // obtain the next name offset by incrementing it atomically
        do {
            nameOffset = nextNameOffset.get();
        } while (!nextNameOffset.compareAndSet(nameOffset, nameOffset + 1L));

        return prefix + '_' + nameOffset;
    }

    /**
     * Creates a wildcard name symmetric to {@link #createRandomName(String)}.
     *
     * @param prefix prefix of the wildcard name
     * @return wildcard name starting with the prefix
     */
    public static String createWildcardName(String prefix) {
        return prefix + "_*";
    }

    // ------------------------------------------------------------------------
    //  RPC service configuration
    // ------------------------------------------------------------------------

    public static long extractMaximumFramesize(Configuration configuration) {
        String maxFrameSizeStr = configuration.getString(AkkaOptions.FRAMESIZE);
        String akkaConfigStr = String.format(SIMPLE_AKKA_CONFIG_TEMPLATE, maxFrameSizeStr);
        Config akkaConfig = ConfigFactory.parseString(akkaConfigStr);
        return akkaConfig.getBytes(MAXIMUM_FRAME_SIZE_PATH);
    }

    // ------------------------------------------------------------------------
    //  RPC service builder
    // ------------------------------------------------------------------------

    /** Builder for {@link AkkaRpcService}. */
    public static class AkkaRpcServiceBuilder {
        //这个类中有3种conf，易混淆。configuration、actorSystemExecutorConfiguration、customConfig。不知为什么要分这么多
        private final Configuration configuration;
        private final Logger logger;
        @Nullable private final String externalAddress;
        @Nullable private final String externalPortRange;

        private String actorSystemName = AkkaUtils.getFlinkActorSystemName();

        @Nullable
        private BootstrapTools.ActorSystemExecutorConfiguration actorSystemExecutorConfiguration =
                null;

        @Nullable private Config customConfig = null;
        private String bindAddress = NetUtils.getWildcardIPAddress();
        @Nullable private Integer bindPort = null;

        /** Builder for creating a remote RPC service. */
        private AkkaRpcServiceBuilder(
                final Configuration configuration,
                final Logger logger,
                @Nullable final String externalAddress,
                final String externalPortRange) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress =
                    externalAddress == null
                            ? InetAddress.getLoopbackAddress().getHostAddress()
                            : externalAddress;
            this.externalPortRange = Preconditions.checkNotNull(externalPortRange);
        }

        /** Builder for creating a local RPC service. */
        private AkkaRpcServiceBuilder(final Configuration configuration, final Logger logger) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress = null;
            this.externalPortRange = null;
        }

        public AkkaRpcServiceBuilder withActorSystemName(final String actorSystemName) {
            this.actorSystemName = Preconditions.checkNotNull(actorSystemName);
            return this;
        }

        public AkkaRpcServiceBuilder withActorSystemExecutorConfiguration(
                final BootstrapTools.ActorSystemExecutorConfiguration
                        actorSystemExecutorConfiguration) {
            this.actorSystemExecutorConfiguration = actorSystemExecutorConfiguration;
            return this;
        }

        public AkkaRpcServiceBuilder withCustomConfig(final Config customConfig) {
            this.customConfig = customConfig;
            return this;
        }
        //看到这个this不必奇怪，因为AkkaRpcServiceBuilder是作为AkkaRpcServiceUtils内的一个静态类
        //就是为了给bindAddress赋值
        public AkkaRpcServiceBuilder withBindAddress(final String bindAddress) {
            this.bindAddress = Preconditions.checkNotNull(bindAddress);
            return this;
        }
        //给bindPort赋值
        public AkkaRpcServiceBuilder withBindPort(int bindPort) {
            Preconditions.checkArgument(
                    NetUtils.isValidHostPort(bindPort), "Invalid port number: " + bindPort);
            this.bindPort = bindPort;
            return this;
        }

        public AkkaRpcService createAndStart() throws Exception {
            //这里传的是function，此function是constructor。需要一个actorSystem，关于rpcService的配置。（泛型是2参和返回）
            //这个::new是个什么呢？
            return createAndStart(AkkaRpcService::new);
        }

        public AkkaRpcService createAndStart(
                BiFunction<ActorSystem, AkkaRpcServiceConfiguration, AkkaRpcService> constructor)
                throws Exception {
            //获取线程池并行度配置，一般而言，到这应当是null吧，然后，通过configuration再来获取，这个conf在构造builder的时候传入了
            //这里为什么要用forkJoin的名字，和java中自带forkjoin有什么关系？
            if (actorSystemExecutorConfiguration == null) {
                actorSystemExecutorConfiguration =
                        BootstrapTools.ForkJoinExecutorConfiguration.fromConfiguration(
                                configuration);
            }

            final ActorSystem actorSystem;

            if (externalAddress == null) {
                //如果没有配置外部访问地址，创建本地ActorSystem。
                // create local actor system
                actorSystem =
                        BootstrapTools.startLocalActorSystem(
                                configuration,
                                actorSystemName,
                                logger,
                                actorSystemExecutorConfiguration,
                                customConfig);
            } else {
                //否则创建一个远程ActorSystem。（这里竟然可以通过外部访问地址，创建远程的ActorSystem）
                //（创建一个可远程调用的ActorSystem实例，意思是在远程创建一个ActorSystem？不懂2个ActorSystem的交互）
                //这里明显不同的是传了4个address、port相关的
                // create remote actor system
                actorSystem =
                        BootstrapTools.startRemoteActorSystem(
                                configuration,
                                actorSystemName,
                                externalAddress,
                                externalPortRange,
                                bindAddress,
                                Optional.ofNullable(bindPort),
                                logger,
                                actorSystemExecutorConfiguration,
                                customConfig);
            }
            //返回AkkaRpcService实例（那么这个实例，对应的actorSystem可以是在远程上的，也可以是在本地上的？）
            return constructor.apply(
                    actorSystem, AkkaRpcServiceConfiguration.fromConfiguration(configuration));
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private AkkaRpcServiceUtils() {}
}
