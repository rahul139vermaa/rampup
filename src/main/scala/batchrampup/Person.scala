package batchrampup

import java.io.StringWriter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sun.xml.bind.api.TypeReference


/**
  * Created by rahul on 11/11/16.
  */

case class Person(name: String, age: Long)


object PersonUtils{
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)


  def toJson(persons: List[Person])={
    val out = new StringWriter()
    mapper.writeValue(out, persons)
    out.toString
  }

  /*def toJson(persons: List[Person])={
    val out = new StringWriter()
    mapper.writeValue(out, persons)
    out.toString
  }
*/
  def fromJson(json:String):List[Person]={
    mapper.readValue(json, classOf[Array[Person]]).toList
  }
}


