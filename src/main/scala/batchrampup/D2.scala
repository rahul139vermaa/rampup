package batchrampup

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by rahul on 15/11/16.
  */
object D2 {
  val sc ={
    val conf = new SparkConf()
    new SparkContext(conf)
  }

}
