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

package org.apache.flink.runtime.rpc;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interface for rpc services. An rpc service is used to start and connect to a {@link RpcEndpoint}.
 * Connecting to a rpc server will return a {@link RpcGateway} which can be used to call remote
 * procedures.
 */
//一个进程拥有一个RpcService。
public interface RpcService {

    /**
     * Return the hostname or host address under which the rpc service can be reached. If the rpc
     * service cannot be contacted remotely, then it will return an empty string.
     *
     * @return Address of the rpc service or empty string if local rpc service
     */
    //获取rpc服务的地址。如果是本地rpc服务则为空
    String getAddress();

    /**
     * Return the port under which the rpc service is reachable. If the rpc service cannot be
     * contacted remotely, then it will return -1.
     *
     * @return Port of the rpc service or -1 if local rpc service
     */
    //获取rpc服务端口号，如果是本地rpc服务则-1
    int getPort();

    /**
     * Connect to a remote rpc server under the provided address. Returns a rpc gateway which can be
     * used to communicate with the rpc server. If the connection failed, then the returned future
     * is failed with a {@link RpcConnectionException}.
     *
     * @param address Address of the remote rpc server
     * @param clazz Class of the rpc gateway to return
     * @param <C> Type of the rpc gateway to return
     * @return Future containing the rpc gateway or an {@link RpcConnectionException} if the
     *     connection attempt failed
     */
    //根据提供的地址，连接到远程RPC服务
    //返回C的gateway，用于和远程通信（实际上应当是返回一个future，通过future的get可以获得相应的c）
    <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz);

    /**
     * Connect to a remote fenced rpc server under the provided address. Returns a fenced rpc
     * gateway which can be used to communicate with the rpc server. If the connection failed, then
     * the returned future is failed with a {@link RpcConnectionException}.
     *
     * @param address Address of the remote rpc server
     * @param fencingToken Fencing token to be used when communicating with the server
     * @param clazz Class of the rpc gateway to return
     * @param <F> Type of the fencing token
     * @param <C> Type of the rpc gateway to return
     * @return Future containing the fenced rpc gateway or an {@link RpcConnectionException} if the
     *     connection attempt failed
     */
    //Fence是防止脑裂的机制，这里是创建一个具有Fence功能的网关
    <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz);

    /**
     * Start a rpc server which forwards the remote procedure calls to the provided rpc endpoint.
     *
     * @param rpcEndpoint Rpc protocol to dispatch the rpcs to
     * @param <C> Type of the rpc endpoint
     * @return Self gateway to dispatch remote procedure calls to oneself
     */
    //启动rpc服务，可将接收到的远程请求发送给rpcEndpoint处理。（这个rpcserver每个rpcendpoint有一个，是rpcendpoint的代理对象）
    <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint);

    /**
     * Fence the given RpcServer with the given fencing token.
     *从下面的翻译知，会将fencing token附加到提供的值中，要求对应的endpoint要继承fencedrpcendpoint
     * <p>Fencing the RpcServer means that we fix the fencing token to the provided value. All RPCs
     * will then be enriched with this fencing token. This expects that the receiving RPC endpoint
     * extends {@link FencedRpcEndpoint}.
     *
     * @param rpcServer to fence with the given fencing token
     * @param fencingToken to fence the RpcServer with
     * @param <F> type of the fencing token
     * @return Fenced RpcServer
     */
    //和上面一样，就是有了防脑裂功能
    <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken);

    /**
     * Stop the underlying rpc server of the provided self gateway.
     *
     * @param selfGateway Self gateway describing the underlying rpc server
     */
    //停止RPC服务（这个是停止selfGateway对应的rpcserver）
    void stopServer(RpcServer selfGateway);

    /**
     * Trigger the asynchronous stopping of the {@link RpcService}.
     *
     * @return Future which is completed once the {@link RpcService} has been fully stopped.
     */
    //异步停止rpc服务（这个是停止rpcservice，和直接停止rpcserver还是不同）
    CompletableFuture<Void> stopService();

    /**
     * Returns a future indicating when the RPC service has been shut down.
     *
     * @return Termination future
     */
    //rpcservice停止时，返回此completeFuture
    CompletableFuture<Void> getTerminationFuture();

    /**
     * Gets the executor, provided by this RPC service. This executor can be used for example for
     * the {@code handleAsync(...)} or {@code thenAcceptAsync(...)} methods of futures.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @return The execution context provided by the RPC service
     */
    //获取RPC服务执行线程，可用于futures的handleAsync等异步逻辑执行（记得线程池就是executorService来着？）
    Executor getExecutor();

    /**
     * Gets a scheduled executor from the RPC service. This executor can be used to schedule tasks
     * to be executed in the future.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @return The RPC service provided scheduled executor
     */
    //获取rpc服务定时调度线程
    ScheduledExecutor getScheduledExecutor();

    /**
     * 在指定的调度延迟后，执行rpcservice的execution context中的线程任务（继承了Runnable就是一个线程任务）
     * Execute the runnable in the execution context of this RPC Service, as returned by {@link
     * #getExecutor()}, after a scheduled delay.
     *
     * @param runnable Runnable to be executed
     * @param delay The delay after which the runnable will be executed
     */
    //设置一个定时任务，定时任务可以由ScheduledExecutor执行
    ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit);

    /**
     * //意思为可以用一个主线程外的rpcEndpoint线程（executor）执行任务。
     * Execute the given runnable in the executor of the RPC service. This method can be used to run
     * code outside of the main thread of a {@link RpcEndpoint}.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @param runnable to execute
     */
    //在rpc服务线程池中运行runnable。（各个RpcEndpoint的线程就是运行在rpc服务线程池的）
    void execute(Runnable runnable);

    /**
     * Execute the given callable and return its result as a {@link CompletableFuture}. This method
     * can be used to run code outside of the main thread of a {@link RpcEndpoint}.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @param callable to execute
     * @param <T> is the return value type
     * @return Future containing the callable's future result
     */
    //在rpc服务线程池中异步运行callable任务，异步结果以CompletableFuture形式返回
    <T> CompletableFuture<T> execute(Callable<T> callable);
}
