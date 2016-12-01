package yarnapp

import java.util.Collections

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Resource, LocalResource, ContainerLaunchContext}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import collection.JavaConverters._
import yarnapp.Utils._

import scala.collection.mutable.ListBuffer

/**
  * Created by rahul on 24/11/16.
  */
object Client {
  def main(args: Array[String]) {

    implicit val conf = new YarnConfiguration()
    val jarPath = args(0)
    val numberOfInstances = args(1).toInt


    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()


    /*Connect to ResourceManager and request for a new application ID:
The client connects to the ResourceManager service and requests a new
application. The response of the request (that is, YarnClientApplication
– GetNewApplicationResponse) contains a new application ID and the
minimum and maximum resource capability of the cluster.*/
    val app = client.createApplication()


    //app.getNewApplicationResponse.getMaximumResourceCapability //



/*Define ContainerLaunchContext for Application Master: The first container
for an application is the ApplicationMaster's container. The client defines
a ContainerLaunchContext, which contains information to start the
ApplicationMaster service*/
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    //application master is a just java program with given commands
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" +
        " yarnapp.ApplicationMaster" +
        "  " + jarPath + "   " + numberOfInstances + " " +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)


    /*Set up jar for ApplicationMaster: The NodeManager should be
able to locate the ApplicationMaster jar file. The jar file is placed
on HDFS and is accessed by NodeManager as a LocalResource */
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpLocalResource(new Path(jarPath), appMasterJar)
    amContainer.setLocalResources(Collections.singletonMap("helloworld.jar", appMasterJar))

    //setup env to get all yarn and hadoop classes in classpath
    val env = collection.mutable.Map[String, String]()
    setUpEnv(env)
    amContainer.setEnvironment(env.asJava)

    //specify resource requirements
    //Here we are telling to yarn that we need 300 mb of memory and one cpu to run our application master.
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(300) //in MB
    resource.setVirtualCores(1)

    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName("Hello world yarn App")
    appContext.setAMContainerSpec(amContainer) //CLC
    appContext.setResource(resource)
    appContext.setQueue("default")

    val appId = appContext.getApplicationId
    println("submitting application id" + appId)
    client.submitApplication(appContext)

    /*Some points
    *  A record of accepted applications is written to persistent storage and recovered in case of RM restart or failure.
    *
    *
    *
    * */


  }


}


