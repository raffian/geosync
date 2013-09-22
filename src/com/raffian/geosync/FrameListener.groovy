package com.raffian.geosync

import org.zeromq.ZContext
import org.zeromq.ZFrame
import org.zeromq.ZMQ.Socket
import org.zeromq.ZThread.IAttachedRunnable
import com.raffian.geosync.base.GeoSyncBase

public class FrameListener extends GeoSyncBase implements IAttachedRunnable{

   public FrameListener( String listenerFor){
      clazzname = this.class.simpleName
      name = listenerFor
   }

   @Override
   public void run(Object[] args, ZContext ctx, Socket pipe) {
      while (true){
         ZFrame frame = ZFrame.recvFrame(pipe)
         if (frame){
            def msg = new String(frame.getData())
            log("${msg}")
            frame.destroy()
         }
      }
   }
}
