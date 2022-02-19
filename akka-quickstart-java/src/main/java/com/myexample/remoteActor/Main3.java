package com.myexample.remoteActor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Author: gaohuibin
 * Date: 2022/2/18 11:39
 * Desc:
 * 演示在远程（Main2）创建一个Actor（ToCreateRemoteActor）
 * Version: 1.0
 */
public class Main3 {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("remoteActor/create_remote.conf");
        // Create an Akka system
        //根据配置，这个ActorSystem占用的是2553端口
        //配置中，deployment中还有关于远端AS的信息，表示在哪个AS上建立
        //（这个配置文件中，有在此机器上创建AS，也有远程AS的信息，但/toCreateActor是用来做什么，不知。而且和下面要建立的actor重名了。）
        //（现在知道了，作用是指定创建的名为此的actor是在远端建立，而不是本地，/toCreateActor下面的remote就是表明了要建立的远端AS位置）
        ActorSystem system = ActorSystem.create("main3", config);
        //要求在远程上创建这个actor,这个actor的名字就叫toCreateActor
        ActorRef actor = system.actorOf(Props.create(ToCreateRemoteActor.class), "toCreateActor");
        //向远端actor发送消息
        actor.tell("I'm created!", ActorRef.noSender());
    }
}
