package batchrampup

import java.io.BufferedReader
import java.util.Calendar
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
  * Created by rahul on 15/11/16.
  */
object D2WithDF {
    import batchrampup.D2._
    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits._


  def Ob2={

    /*Ob2. Use following methods to explore different operations on this List object to:
       Obj2.1 In the list add the year-of-birth to get new list - List[Person, Int].
       Obj2.2 Find average age of Person using 'age' class member.
       Obj2.3 Find sum of  age of Person using 'age' class member.
       Obj2.4 Group people with their name's initial character, and calculate the average age for each initial.  */


    val personsDF = sqlContext.read.json(Constants.HDFS_URI+Constants.FILE_PATH).as[Person].toDF


    //2.1
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    val modDF = personsDF.select(personsDF("age"),personsDF("name"),-personsDF("age")+currentYear as "YOB")

    //2.2
    /*
    Obj2.2 Find average age of Person using 'age' class member.
    Jobs - 1
    Stages -2
    Tasks - 3
    Partitions - 2
    */

    val resAvg = modDF.select(avg(modDF("age")))
    resAvg.show()


    //2.3
    /*
    Obj2.3 Find sum of  age of Person using 'age' class member.
    Jobs - 1
    Stages - 2
    Tasks - 3
    Partitions - 2
     */

    val resSum = modDF.select(sum(modDF("age")) )
    resSum.show()
    //2.4
    /*
    Obj2.4 Group people with their name's initial character, and calculate the average age for each initial.
    Jobs - 2
    Stages - 3
    Tasks - 202
    Partitions - 199
     */

    val avgAgeGroupByInitial= modDF.groupBy(modDF("name").substr(0,1)).avg("age")
    avgAgeGroupByInitial.show()


  }
}
