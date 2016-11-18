package batchrampup

import scala.io.Source

/**
  * Created by rahul on 15/11/16.
  */
object Constants {
    println("please enter File path without host port")
    val FILE_PATH ={val str=Source.stdin.getLines().next;if(str!=null && !str.isEmpty)str else "/home/person.json"}
    println("please enter URI i-e protocol://host:port")
    val HDFS_URI ={val str=Source.stdin.getLines().next;if(str!=null && !str.isEmpty)str else "hdfs://10.41.66.215:9000"}

}
