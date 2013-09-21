package com.raffian.geosync

import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZThread
import org.zeromq.ZMQ.Socket
import org.zeromq.ZThread.IAttachedRunnable

class Queue extends GeoSyncProxy implements IAttachedRunnable {

   public Queue(){
      clazzname = this.class.simpleName
   }

   @Override
   public void run(Object[] args, ZContext ctx, Socket pipe) {
      Socket input = ctx.createSocket(ZMQ.ROUTER)
      input.bind( proxyIn)
      Socket output = ctx.createSocket(ZMQ.DEALER)
      output.bind( proxyOut)

      log("proxy started")
      ZMQ.proxy( input, output, ZThread.fork(ctx, new FrameListener(clazzname)))
   }
}
