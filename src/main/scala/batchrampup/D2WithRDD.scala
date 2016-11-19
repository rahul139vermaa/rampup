package batchrampup

import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rahul on 15/11/16.
  */
object D2WithRDD {

  import batchrampup.D2._

  def Ob1 = {
    //done previously
    /*Ob1. Create an object - List[Person] with 1 million objects
      (in case the shell crashes, have lesser objects) with random name and ages(range 1-100).
      Save this list to HDFS as json/parquet/csv file format.*/
  }

  def Ob2 = {

    /*Ob2. Use following methods to explore different operations on this List object to:
       Obj2.1 In the list add the year-of-birth to get new list - List[Person, Int].
       Obj2.2 Find average age of Person using 'age' class member.
       Obj2.3 Find sum of  age of Person using 'age' class member.
       Obj2.4 Group people with their name's initial character, and calculate the average age for each initial.  */


    val persons = sc.parallelize(
      PersonUtils.fromJson(sc.textFile(Constants.HDFS_URI + Constants.FILE_PATH).first)
    )

    //2.1
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    val modList = persons.map(p => (p, currentYear - p.age))
    //println(modList.collect().toList)

    //2.3
    /*
    Obj2.3 Find sum of  age of Person using 'age' class member.

    Jobs - 1
    Stages - 1
    Tasks - 2
    Partitions - 2
    */

    val sum = modList.map(_._1.age).sum
    //OR (seems same as sum will use fold method)
    val sumUsingReduce = modList.map(_._1.age).reduce((a, b) => a + b)
    println(sum)
    println(sumUsingReduce)

    //2.2
    /*Obj2.2 Find average age of Person using 'age' class member.

      Jobs - 1
      Stages -1
      Tasks - 2
      Partitions - 2   */

    val avg = sum / modList.count
    println(avg)

    //2.4
    /*
    Obj2.4 Group people with their name's initial character, and calculate the average age for each initial.
    Jobs - 1
    Stages - 2
    Tasks - 4
    Partitions - 2
    */
    val avgAgeGroupByInitial = {
      val pairRdd = modList.map(x => (x._1.name.head, x._1.age))
      val gbk = pairRdd.groupByKey
      gbk.map(x => (x._1, x._2.sum / x._2.size)).collect
    }
    println(avgAgeGroupByInitial.toMap)


  }
}
