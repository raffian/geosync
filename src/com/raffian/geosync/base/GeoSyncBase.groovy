package com.raffian.geosync.base

abstract class GeoSyncBase {

   def name
   def clazzname

   protected boolean threadNotInterrupted(){
      return !Thread.currentThread().isInterrupted()
   }

   def log( msg){
      println "[$clazzname ($name)]  $msg"
   }
}
