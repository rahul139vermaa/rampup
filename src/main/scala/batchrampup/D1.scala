package batchrampup

import java.util.Calendar
import scala.collection.mutable.ListBuffer

/**
  * Created by rahul on 15/11/16.
  */
object D1 {

  def Ob1(loop:Int) = {
    /*Ob1. Create an object - List[Person] with 1 million objects
      (in case the shell crashes, have lesser objects) with random name and ages(range 1-100).
      Save this list to HDFS as json/parquet/csv file format.*/

    def write={
      val r = scala.util.Random
      val personBuffer = ListBuffer[Person]()
      for (i <- 0 to loop) {
        personBuffer +=  Person(r.nextPrintableChar+"mid"+i, r.nextInt(100))
      }
      val persons = personBuffer.toList
      val jsList=PersonUtils.toJson(persons)
      HDFSFileService.writeTofile(Constants.FILE_PATH,jsList.toString(),true)
    }

    write
  }

  def Ob2 = {

    /*Ob2. Use following methods to explore different operations on this List object to:
       Obj2.1 In the list add the year-of-birth to get new list - List[Person, Int].
       Obj2.2 Find average age of Person using 'age' class member.
       Obj2.3 Find sum of  age of Person using 'age' class member.
       Obj2.4 Group people with their name's initial character, and calculate the average age for each initial.  */

    def read ={
      HDFSFileService.readFile(Constants.FILE_PATH)
    }

    val content = read
    val persons = PersonUtils.fromJson(content)

    //2.1
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    val modList = persons.map(p => (p, currentYear - p.age))

    //2.3
    val sum = modList.map(_._1.age).sum
    println(sum)

    //2.2
    val avg = sum / modList.size
    println(avg)

    //2.4
    val avgAgeGroupByInitial = {
      modList.groupBy(_._1.name.head).
        map(
          entry => entry._1 -> (entry._2.map(_._1.age).sum / entry._2.size)
        )
    }
    println(avgAgeGroupByInitial)
  }
}
