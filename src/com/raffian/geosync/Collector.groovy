package com.raffian.geosync

import org.zeromq.ZContext
import org.zeromq.ZFrame
import org.zeromq.ZMQ
import org.zeromq.ZThread
import org.zeromq.ZMQ.Socket
import org.zeromq.ZThread.IAttachedRunnable
import org.zmq.base.GeoSyncProxy

class Collector extends GeoSyncProxy implements IAttachedRunnable {

   public Collector(){
      clazzname = this.class.simpleName
   }

   @Override
   public void run(Object[] args, ZContext ctx, Socket pipe) {
      Socket input = ctx.createSocket(ZMQ.XSUB)
      input.bind( proxyIn)
      Socket output = ctx.createSocket(ZMQ.XPUB)
      output.bind( proxyOut)

      log("proxy started")
      ZMQ.proxy( input, output, ZThread.fork(ctx, new Listener()))
   }
}
