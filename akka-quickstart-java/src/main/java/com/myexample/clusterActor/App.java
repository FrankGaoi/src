package com.myexample.clusterActor;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: gaohuibin
 * Date: 2022/2/18 16:05
 * Desc:
 * Version: 1.0
 */
public class App
{
    public static void main( String[] args )
    {
        //可以输参，也可以默认
        if(args.length==0)
            startup(new String[] {"2551", "2552", "0"});
        else
            startup(args);
    }

    public static void startup(String[] ports){
        //获取一个固定线程数量的线程池，数量和传入的端口数一致
        ExecutorService pool = Executors.newFixedThreadPool(ports.length);
        for(String port : ports){
            //submit是将线程任务提交给线程
            pool.submit(()->{
                // Using input port to start multiple instances
                //这里怎么需要两个neety.tcp和artery的端口呢？
                //配置上还与cluster相关的，应当是指如果在指定2个port启动的，就作为seedNode
                Config config = ConfigFactory.parseString(
                  "akka.remote.netty.tcp.port=" + port + "\n" +
                    "akka.remote.artery.canonical.port=" + port)
                  .withFallback(ConfigFactory.load("clusterActor/application.conf"));

                // Create an Akka system
                ActorSystem system = ActorSystem.create("ClusterSystem", config);

                // Create an actor
                system.actorOf(Props.create(SimpleClusterListener.class), "ClusterListener");
            });
        }
    }
}
