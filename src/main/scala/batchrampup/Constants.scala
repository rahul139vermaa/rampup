package batchrampup

import scala.io.Source

/**
  * Created by rahul on 15/11/16.
  */
object Constants {
    /*println("please enter File path without host port")
    var FILE_PATH ={val str=Source.stdin.getLines().next;if(str!=null && !str.isEmpty)str else "/home/person.json"}
    println("please enter URI i-e protocol://host:port")
    var HDFS_URI ={val str=Source.stdin.getLines().next;if(str!=null && !str.isEmpty)str else "hdfs://10.41.66.215:9000"}*/

    var FILE_PATH ="/home/person.json";
    var HDFS_URI = "hdfs://10.41.66.215:9000";

}
