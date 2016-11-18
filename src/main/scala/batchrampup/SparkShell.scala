package batchrampup

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.io.Source

/**
  * Created by rahul on 12/11/16.
  */
object SparkShell {
  /*import play.api.libs.json._
  import scala.collection.mutable.ListBuffer
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.spark.{SparkConf, SparkContext}
  import play.api.libs.json.Json
  import java.io.{BufferedInputStream, BufferedOutputStream}
  import scala.collection.mutable.ListBuffer
  import java.io.BufferedOutputStream
  import java.net.URI
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{Path, FileSystem}
  import scala.io.Source
  import java.util.Calendar

  case class Person(name: String, age: Long)



  object Person {
    //implicit val write=Json.format[batchrampup.Person]
    implicit object PersonWrites extends Format[Person]{

      override def reads(json: JsValue): JsResult[Person] = {
        JsSuccess(Person(json.\("name").get.as[String], json.\("age").get.as[Long]))

      }

      override def writes(person: Person): JsValue = {
        val personSeq = Seq(
          "name" -> JsString(person.name),
          "age" -> JsNumber(person.age)
        )
        JsObject(personSeq)
      }
    }

  }


  //read


  val uri = new URI("hdfs://10.41.66.215:9000")
  val conf= new Configuration()
  val fs = FileSystem.get(uri,conf)

  def readFile(pathString:String)={
    def in ={
      fs.open(new Path(pathString))
    }
    val returnString=Source.createBufferedSource(in).getLines().mkString
    fs.close
    returnString
  }
  def read ={
    readFile("/home/scalaJson")
  }

  ////write

  val r = scala.util.Random
  val personBuffer = ListBuffer[Person]()
  for (i <- 0 to 100000) {
    personBuffer +=  Person("myName" + i, r.nextInt(100).toLong)
  }
  val persons = personBuffer.toList
  val jsList=persons.map(ele => Json.toJson(ele)).toList
  //sc.parallelize(jsList).saveAsTextFile("hdfs://10.41.66.215:9000/home/json")*/



}
