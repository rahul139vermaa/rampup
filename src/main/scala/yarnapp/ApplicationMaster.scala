package yarnapp

import java.util.Collections

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import yarnapp.Utils._
import collection.JavaConverters._

/**
  * Created by rahul on 24/11/16.
  */
object ApplicationMaster {
  def main(args: Array[String]) {
    val jarPath = args(0)
    val n = args(1).toInt
    implicit val conf = new YarnConfiguration()

    // Create a client to talk to the RM
    val rmClient = AMRMClient.createAMRMClient().asInstanceOf[AMRMClient[ContainerRequest]]
    rmClient.init(conf)
    rmClient.start()

    /*Register the attempt with the ResourceManager : The ApplicationMaster
 registers itself to the ResourceManager service. It needs to specify the
hostname, port and a tracking URL for the attempt. After successful
registration, the ResourceManager moves the application state to RUNNING*/
    rmClient.registerApplicationMaster("", 0, "")

    //create a client to talk to NM
    val nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()


    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    //resources needed by each container
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(128)
    resource.setVirtualCores(1)


    /*Define ContainerRequest and add the container's request : The client
defines the execution requirement of worker containers in terms of memory
and cores (org.apache.hadoop.yarn.api.records.Resource). The client
might also specify the priority of the worker containers, a preferred list of
nodes, and racks for resource locality. The client creates a ContainerRequest
reference and adds the requests before calling the allocate() method:*/
    for (i <- 1 to n) {
      val containerAsk = new ContainerRequest(resource, null, null, priority)

      println("asking for " + s"$i")
      rmClient.addContainerRequest(containerAsk)
    }


    var responseId = 0
    var completedContainers = 0

    while (completedContainers < n) {

      val appMasterJar = Records.newRecord(classOf[LocalResource])
      setUpLocalResource(new Path(jarPath), appMasterJar)

      val env = collection.mutable.Map[String, String]()
      setUpEnv(env)



      /*
      * Request allocation, define ContainerLaunchContext and start the containers:
The ApplicationMaster requests the ResourceManager to allocate the required
containers and notifies the ResourceManager about the current progress of the
application. Hence, the value of progress indicator during the first allocation
request is 0. The response from the ResourceManager contains the number of
allocated containers. The ApplicationMaster creates ContainerLaunchContext
for each allocated container and requests the corresponding NodeManager
to start the container. It will wait for the execution of the containers. In this
example, the command executed to launch the containers is specified as the
first argument for the ApplicationMaster (the /bin/date command):
      * */


      val response = rmClient.allocate(responseId)

      /*
      *In response to AM requests, the RM generates containers together with tokens that grant access to resources.
      * used for authentication purpose with NM => response.getNMTokens
      *
      *
      *
      * */




      responseId += 1


      for (container <- response.getAllocatedContainers.asScala) {
        val ctx =
          Records.newRecord(classOf[ContainerLaunchContext])
        ctx.setCommands(
          List(
            "$JAVA_HOME/bin/java" +
              " -Xmx256M -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 " +
              " yarnapp.HelloWorld" +
              " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
              " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
          ).asJava
        )

        ctx.setLocalResources(Collections.singletonMap("helloworld.jar",
          appMasterJar))
        ctx.setEnvironment(env.asJava)

        System.out.println("Launching container " + container)
        nmClient.startContainer(container, ctx)

      }
      for (status <- response.getCompletedContainersStatuses.asScala) {
        completedContainers += 1
        println("completed container " + status.getContainerId())
      }
      Thread.sleep(100);

    }

    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "");
  }


}
