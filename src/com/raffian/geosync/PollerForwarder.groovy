package com.raffian.geosync

import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMsg
import org.zeromq.ZThread
import org.zeromq.ZMQ.Socket
import org.zeromq.ZThread.IAttachedRunnable
import org.zmq.base.GeoSyncProxy

class PollerForwarder extends GeoSyncProxy implements IAttachedRunnable {

   def pollAddresses

   public PollerForwarder(){
      clazzname = this.class.simpleName
   }

   @Override
   public void run(Object[] args, ZContext ctx, Socket pipe) {
      //start proxy first
      ZThread.fork(
            ctx, new Queue(
            name:"${name} Queue",
            proxyIn:proxyIn,
            proxyOut:proxyOut))

      ZMQ.Poller multiPoller = new ZMQ.Poller( pollAddresses.size())
      pollAddresses.each{ addr ->
         Socket subscriber = ctx.createSocket( ZMQ.SUB)
         subscriber.connect( addr)
         subscriber.subscribe( "".getBytes())
         multiPoller.register(subscriber, ZMQ.Poller.POLLIN)
      }

      //forward polled messages to this socket
      Socket pusher = ctx.createSocket(ZMQ.PUSH)
      pusher.connect( proxyIn)

      log("polling...")
      while ( threadNotInterrupted()) {
         //blocks until messages arrive from any polled socket
         multiPoller.poll()

         //iterate the sockets to see which one has messages
         multiPoller.items.each{ polledSocket ->
            if (polledSocket.isReadable()) {
               ZMsg msg = ZMsg.recvMsg( polledSocket.getSocket())
               log( "received message '${msg}', forwarding to ${proxyIn}...")
               msg.send( pusher)
            }
         }
      }
   }
}

