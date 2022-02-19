package com.myexample.remoteActor;

import akka.actor.AbstractActor;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Author: gaohuibin
 * Date: 2022/2/17 17:22
 * Desc: a toFindRemoteActor can be found by other remoteActor.
 * This actor can be deployed on a remote server.we use port 2551 imitate the remote machine.
 * 模拟一个放在远程上用于被寻找的actor
 * Version: 1.0
 */
public class ToFindRemoteActor extends AbstractActor {

    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    @Override
    public void preStart() throws Exception {
        log.info("ToFindRemoteActor is starting");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
          .match(String.class, msg->{
              log.info("Msg received: {}", msg);
          })
          .build();
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.parseString(
          "akka.remote.netty.tcp.port=" + 2551)
          .withFallback(ConfigFactory.load("remoteActor/remote.conf"));

        // Create an Akka system
        ActorSystem system = ActorSystem.create("sys", config);

        // Create an actor
        system.actorOf(Props.create(ToFindRemoteActor.class), "toFind");
    }
}
