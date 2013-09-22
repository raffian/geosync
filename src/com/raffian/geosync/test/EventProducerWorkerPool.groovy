package com.raffian.geosync.test

import java.util.concurrent.Executors
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMsg
import org.zeromq.ZThread
import org.zeromq.ZMQ.Socket

import com.raffian.geosync.base.GeoSyncBase

/**
 * To run:
 * > groovy -cp [classpath] com/raffian/geosync/test/EventProducerWorkerPool.groovy tcp://localhost:5510
 * Make sure classpath has Groovy and ZeroMq dependencies.
 */
public class EventProducerWorkerPool extends GeoSyncBase implements Runnable {
   def WORKERS = 5   
   WorkerPool workerPool
   static String publishAddress

   public EventProducerWorkerPool(n, address){
      clazzname = this.class.simpleName
      name = n
      publishAddress = address
      workerPool = new WorkerPool( WORKERS)
   }

   @Override
   public void run() {
      log( "producer started on: " + publishAddress)
      
      //send some test events
      for( event in 1..25){    
         workerPool.doTask( 
            new CacheEventTask( "key-${event}", "value-${event}"))
         log("Sent event ${event}")
         Thread.currentThread().sleep(1500)
      }
   }

   /**
    * A worker thread pool based on Executors fixed thread pool model.
    * Handles tasks of type CacheEventTask.
    */
   class WorkerPool {
      def threadPool

      public WorkerPool( threads){
         threadPool =
            Executors.newFixedThreadPool(
               threads, Executors.defaultThreadFactory())
      }

      def doTask( CacheEventTask task){
         threadPool.execute( task)
      }
   }


   class CacheEventTask implements Runnable {
      def eventDetails
      static ZContext taskCtx = new ZContext()

      //accessing the outer class just for examples sake,
      //production code should get publishAddress from a config file
      static String publishAddress =
         EventProducerWorkerPool.publishAddress

      public CacheEventTask( String... info){
         eventDetails = new ArrayList<String>(Arrays.asList(info))
         //reverse the event details to allow
         //consumer to pop frames in order
         Collections.reverse(eventDetails)
      }

      public void run(){
         ZMsg event = new ZMsg()
         for( String s : eventDetails){
            //log( "pushed->" + s)
            event.push( s)
         }
         event.send( getSocket())
      }

      protected Socket getSocket(){
         threadLocal.get()
      }

      //static ThreadLocal uses a dedicated socket per
      //task thread instance, ready to publish events
      private static final ThreadLocal<ZMQ.Socket> threadLocal =
      new ThreadLocal<Socket>(){
         @Override
         protected Socket initialValue(){
            Socket publisher = taskCtx.createSocket(ZMQ.PUB)
            publisher.connect( publishAddress)           
            return publisher
         }
      }
   }

   static main(args) {
      (new Thread(EventProducerWorkerPool producer =
            new EventProducerWorkerPool("producer", args[0]))).start()
   }
}
