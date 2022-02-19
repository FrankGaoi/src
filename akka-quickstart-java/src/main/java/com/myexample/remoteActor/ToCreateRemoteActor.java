package com.myexample.remoteActor;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Author: gaohuibin
 * Date: 2022/2/18 10:54
 * Desc: 这是个将要在远程上创建起来的Actor,我们要尝试在Main2启动一个Actor，见Main3
 * Version: 1.0
 */
public class ToCreateRemoteActor extends AbstractActor {

    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    @Override
    public void preStart() throws Exception {
        log.info("ToCreateRemoteActor is starting");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
          .match(String.class, msg->{
              log.info("Msg received: {}", msg);
          })
          .build();
    }
}
