package com.myexample.remoteActor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Author: gaohuibin
 * Date: 2022/2/18 14:34
 * Desc:
 * Version: 1.0
 */
public class Main4 {

    public static void main(String[] args) {
        //这里传入0，那么应当是随机占用一个端口？
        Config config = ConfigFactory.parseString(
          "akka.remote.netty.tcp.port=" + 0)
          .withFallback(ConfigFactory.load("remoteActor/remote.conf"));
        // Create an Akka system
        ActorSystem system = ActorSystem.create("main4", config);

        // Find remote actor
        //这种（main3）通过在远程（main2）上创建的actor，也需要通过main3来进行访问。
        //虽然从日志上来看，main3没什么反映，还是在main2上actor做出了动作
        //结论：虽然RemoteActor是创建在远程机器（main2）上，但是想要查询它，还需要向创建者（main3）发出请求
        ActorSelection toFind = system.actorSelection("akka.tcp://main3@127.0.0.1:2553/user/toCreateActor");
        toFind.tell("I'm alive!", ActorRef.noSender());
    }
}
