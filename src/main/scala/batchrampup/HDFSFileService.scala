package batchrampup

import java.io.BufferedOutputStream
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.io.Source

/**
  * Created by rahul on 13/11/16.
  */
object HDFSFileService{

  val uri = new URI(Constants.HDFS_URI)
  val conf= new Configuration()
  val fs = FileSystem.get(uri,conf)

  def readFile(pathString:String)={
    val in ={
      val inStream=fs.open(new Path(pathString))
      inStream
    }
    val returnString=Source.createBufferedSource(in).getLines().mkString
    in.close
    //fs.close
    returnString
  }

  def writeTofile(pathString:String,content:String,overrideIfExist:Boolean)={
    val os={
      val output = fs.create(new Path(pathString),overrideIfExist)
      new BufferedOutputStream(output)
    }
    os.write(content.getBytes())
    os.close
    //fs.close
  }
}
