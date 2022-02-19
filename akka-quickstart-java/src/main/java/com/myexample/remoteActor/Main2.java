package com.myexample.remoteActor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Author: gaohuibin
 * Date: 2022/2/18 11:35
 * Desc:
 * 演示寻找一个远程actor。
 * 启动一个remote AS,向远端发消息。占用2551
 * 并且这个remote AS将被Main3要求建立actor
 * Version: 1.0
 */
public class Main2 {

    public static void main(String[] args) {
        Config config = ConfigFactory.load("remoteActor/remote.conf");
        // Create an Akka system
        ActorSystem system = ActorSystem.create("main2", config);

        // Find remote actor
        ActorSelection toFind = system.actorSelection("akka.tcp://sys@127.0.0.1:2551/user/toFind");
        toFind.tell("hello", ActorRef.noSender());
    }
}
