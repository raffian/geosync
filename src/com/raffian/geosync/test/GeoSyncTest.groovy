package com.raffian.geosync.test

import org.zeromq.ZContext
import org.zeromq.ZThread
import com.raffian.geosync.Collector
import com.raffian.geosync.PollerForwarder


/**
 * Emulates three geographic locations each using an 
 * event collector, poller/forwarder, and two event consumer.
 * TODO - add even producers
 */

class GeoSyncTest {

   static main(args) {

      //network partition #1
      ZContext ctx1 = new ZContext() 
      //pullers
      ZThread.fork(
         ctx1, 
         new EventConsumer(name:"pull1__np1", address:"tcp://localhost:5130"))
      ZThread.fork(
         ctx1, 
         new EventConsumer(name:"pull2__np1", address:"tcp://localhost:5130"))
      //proxies
      
      ZThread.fork(ctx1,
            new PollerForwarder(
            name:"pollr__np1", 
            proxyIn:"inproc://events-np1", 
            proxyOut:"tcp://localhost:5130", 
            pollAddresses:["tcp://localhost:5220","tcp://localhost:5320"]))
      ZThread.fork(
         ctx1, 
         new Collector(
            name:"proxyOut_np1", 
            proxyIn:"tcp://localhost:5110", 
            proxyOut:"tcp://localhost:5120"))

      //network partition #2
      ZContext ctx2 = new ZContext()
      //pullers
      ZThread.fork(
         ctx2, 
         new EventConsumer(name:"pull1__np2", address:"tcp://localhost:5230"))
      ZThread.fork(
         ctx2, 
         new EventConsumer(name:"pull2__np2", address:"tcp://localhost:5230"))
      //proxies
      ZThread.fork(ctx2,
            new PollerForwarder(
            name:"pollr__np2",
            proxyIn:"inproc://events-np2",
            proxyOut:"tcp://localhost:5230",
            pollAddresses:["tcp://localhost:5120","tcp://localhost:5320"]))
      ZThread.fork(
         ctx2, 
         new Collector(
            name:"proxyOut_np2", proxyIn:"tcp://localhost:5210", proxyOut:"tcp://localhost:5220"))

      //network partition #3
      ZContext ctx3 = new ZContext()
      //pullers
      ZThread.fork(
         ctx3, 
         new EventConsumer(name:"pull1__np3", address:"tcp://localhost:5330"))
      ZThread.fork(
         ctx3, 
         new EventConsumer(name:"pull2__np3", address:"tcp://localhost:5330"))
      //proxies
      ZThread.fork(ctx3,
            new PollerForwarder(
            name:"pollr__np3",
            proxyIn:"inproc://events-np3",
            proxyOut:"tcp://localhost:5330",
            pollAddresses:["tcp://localhost:5120","tcp://localhost:5220"]))
      ZThread.fork(
         ctx3, 
         new Collector(
            name:"proxyOut_np3",
            proxyIn:"tcp://localhost:5310", 
            proxyOut:"tcp://localhost:5320"))
   }
}

