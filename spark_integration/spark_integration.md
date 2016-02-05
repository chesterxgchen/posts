# Predictive Analytics Platform Spark Integration

 
Chester Chen
Alpine Data
Tuesday, November 3, 2015
 
 
In this blog, I will discuss how to integrate Apache Spark in a web-based analytics system. Part of the content is discussed in my recent talk at [2015 Big Data Scala Conference](https://www.youtube.com/watch?v=4pTmhmcLWRg). 
You can also check out my [notes](http://alpinedata.com/wp-content/uploads/2015/12/Spark-1.5.1-Upgrade-Notes.pdf) on Spark 1.5 Integration, where 
discussed issues and solutions we observed while upgrading spark from Spark 1.3.1 to Spark 1.5.1.
 
Before we dive into the Spark integration, let us step back to discuss the general system architecture and see where Spark integration fits in.
 
Alpine Data provides a web-based enterprise analytics platform that services different users, from data scientists working on data exploration or advanced modeling, to business users who just want to apply the analytics insight to a business use case.
 
The platform consists of a workflow design console, collaboration/business management, and workflow/analytics engine. We access data from a servlet engine, and the data can be from various data sources (such as a Database or Hadoop). 
 
## Alpine Agent Architecture
 
One of challenges we face is that we need to support multiple Hadoop vendors as well as multiple Hadoop versions from the same vendor. Even different Hadoop versions from the same vendor can require different library versions or APIs.  For example, our customers that use Cloudera might be running CHD5.1.2 or CDH5.4.  CDH5.1.2 is built on the Apache Hadoop 2.3 source code, whereas CDH5.4 uses Apache 2.6.  The API signatures changed between Apache 2.4 and 2.6 (for example, the CRC checksum API), and the 3rd party libraries are also different. To support both CHD5.1.2 and CDH5.4, we need to use a different class loader (and possibly a different JVM) for each Hadoop version.

One way to solve this problem is to build different stacks (UI + Hadoop vendor/version). In this case, we build different applications during build time and deploy the applications based on our customer’s requirements. This requires we maintain different applications for each Hadoop version.  For example, Hadoop CDH5.3.x and CDH5.4.x will be two different stacks with different UI front ends.  This is fine for the end-customers unless they have mix of Hadoop clusters, but for test engineering it can be a lot of work to install different stacks for each variation.
 
 
Another way is to use a common UI, but leverage different JVMs for each Hadoop vendor/version. This approach has the benefits of allowing us to easily switch between different data sources (Hortonworks, Cloudera) or different version of the same vendor (CDH5.1, CDH5.4) without leaving the application.

It does add some complexity to the design. We chose the 2nd approach and designed a system we call Agent Architecture. In Agent Architecture, we leverage Akka to enable remote communications and to scale. Each agent is in one JVM that has its own Akka actor system. The agent can be hosted in one machine or across many machines depending on the load balancing needs.

### Web UI Component
 
The UI component is designed as the web-based workflow console that allows user to interact with data regardless the data source.  Some of the functions include
Browse the data from different data sources like a file browser. The data sources can be from HDFS, databases (MPP, JDBC, Redshift) or Hive table etc.
Provide the data pipeline processing and analytics workflow design console. User can drag and drop the data as well as data-analyzing operators into the console, without needing to write a code.
User can run the workflow and visualize the data and results directly from console.
Provide an interactive console to allow user to interact with data via scripts (Hive, Pig, R, python or python)
The plugin management, job managements and monitoring and general admin functions.
 
The Web UI component provides the interface for user to interact with data and data processing.  
 
### Agent
 
The agent hosts the analytic workflow engine and data analytics logic specific for the Hadoop distributor/vendor or vendor.   All Agents can handle database process.  But for Hadoop, only Hadoop version specific agent selected to processing the Hadoop job. The major functions of the agent include
 
Handle Pig, Hive, MapReduce and Spark Jobs
Handle database queries
Process data pipeline workflow via Analytic Workflow Engine

![Image of Agent](https://github.com/chesterxgchen/posts/blob/master/spark_integration/images/agent.png)


## Spark Integration -- Build scripts
 
Believe it or not, developing the build scripts is the most time consuming and difficult part of the Spark integration. This is largely due to SBT .ivy dependency resolving. SBT has to resolve the jar files each time the build scripts changes.
 
The difficulty also comes from the following complexities
* Both the Web UI and Agent components are servlet containers (running Jetty), so directly including the Spark Assembly jar will cause conflicts as the Spark Assembly also contains Jetty to support the Spark UI.
* We use xsbt-web-plugin to support development mode, where we can start and stop web containers without requiring a distributed agent deployment. This means if we have Jetty and Spark on the same container, things are not going to work well.
* Our platform supports both Spark and non-Spark development, such MapReduce, Pig, Hive etc. The dependency for these Hadoop jars may require different versions of third party jars to Spark.
* At runtime, the order of the jar dependencies may cause the class loader to load a different version of the class file to the one you expected.
* org.spark-project repackages Apache artifacts (such Hive and Akka etc) to reduce unwanted dependencies, but the some of the class with the same class name behave differently than the original Apache class (such as hive-exec).
 
 
Each time we upgrade to a new Spark or Hadoop version, it is a struggle to verify that the changes will not affect other functionalities unexpectedly due to changes in the dependencies.
 
Here is what we have developed:
 
Only use Spark-assembly for test

Selectively include the needed Spark module jars. For example, we only include the core, mllib, yarn, network-shuffle, hive and core modules, other unrelated modules are not used as dependency. Exclude the servlet jars from Spark. Add back the needed 3rd party servlet related to the WebUI module only.Carefully examine the order of 3rd party jar dependency
 
Currently, our integration is only limited to Spark Yarn-Cluster mode, so we included the following artifacts from spark-jar. 
 
We are using SBT to build, the Build script is defined in Build.scala
``` 
 
val scalaMajarVersion=”2.10”
 
lazy val ftsc =  "fun,test,setup,clean"
 
def runtimeSparkJars (sparkVersion:String, hadoopVersion: String) : Seq[ModuleID] = {
  val version: String = s"$sparkVersion-hadoop$jarHadoopVersion"
  Seq(
	"org.apache.spark" % s"spark-assembly_$scalaMajorVersion" % version % ftsc,
	"org.apache.spark" % s"spark-core_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-mllib_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-catalyst_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-sql_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-hive_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-yarn_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-tools_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-unsafe_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-network-yarn_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-network-common_$scalaMajorVersion" % version,
	"org.apache.spark" % s"spark-network-shuffle_$scalaMajorVersion" % version
  )
}
```

As you can see the spark-assembly is only used for tests (setup, clean, function and unit tests); while others components are used for compilation (mostly).  Only a subset of the spark components is used here.
 
There are other dependencies needed for Hive to run in Spark, such as data nucleus JDO dependencies. You will need to specify them as well
```
lazy val datanucleusJars  = {
  Seq(
	"org.datanucleus" % "datanucleus-accessplatform-jdo-rdbms" % "3.2.9",
	"org.datanucleus" % "datanucleus-core" % "3.2.10",
	"org.datanucleus" % "datanucleus-api-jdo" % "3.2.6",
	"org.datanucleus" % "datanucleus-jdo-query" % "4.0.4",
	"org.datanucleus" % "datanucleus-rdbms" % "3.2.9"
  )
}
``` 
We will discuss more on jar dependencies at Runtime during spark job submission.
 
 
 
## Spark Deployment modes
 
Depending the resource manager used or Spark installation, there are several Spark deployment modes: mesos, standalone, yarn-client and yarn-cluster mode.
 
* Standalone mode: Spark is installed on all the nodes in the cluster. HA master is through Zookeeper election. Must dedicate whole cluster to Spark. If one company is intend to mix use Map Reduce and Spark, then this is not suitable.  More Spark users are using Spark standalone mode now.
* Mesos mode: Spark was originally developed to demo Mesos. Mesos can manage many different resources including Yarn as well.  Mesos can manage both JVM and non-JVM (like MPI). It has great support for micro-services such as Docker and Marathon.
* Yarn Mode: Leverage Yarn Resource Manager. This is resource manager supported by major Hadoop distribution vendors. Therefore used by all of clients.
 
In the following discussion, we are only interests in Yarn deployment mode as that is the configuration all of our customers have. For Spark, there are yarn-client and yarn-cluster modes.
 
* Yarn-client mode. The Spark job is run in yarn cluster, but Spark driver in on the client side.
* Yarn-cluster mode. Both driver as well the Spark job are run and managed by yarn resource manager.
 
In Yarn mode, each yarn container is self-contained and independent from other containers. Yarn Resource Manager manages resource allocation. You can find out more details here. In each container, one can run any kind of jobs (map/reduce, pig, hive, or Spark).  This allows the cluster to mix Spark job and map reduce jobs.  One benefit with Yarn deployment is that clients do not need dedicated Spark Cluster. Instead, the existing Hadoop Cluster setup for Pig, Hive or Map Reduce Job can also be leveraged for Spark job.
 
As our analytic application is shared by many uses, Yarn-client mode does not work for us as many users will have to share the same JVM hence the same SparkContext.  Spark Requires that one JVM can only have one SparkContext. If the Spark driver crashes due to one of user’s Spark job (ex. launch a program that collect a lot of data on driver side), then it will bring down the JVM and affects other user jobs as well.
 
To isolate different users’ jobs, we choose to use Yarn-Cluster mode.
 
Unlike standalone Spark mode and yarn-client mode, where user can interactive communicate with the Spark running spark job from Spark-shell and getting job progress and status, the yarn-cluster mode is essentially a batch job: you wait until it completes or fails.
 
This is not ideal; we would like to monitor the job progress, logs, debugging statements, getting exceptions, and stop the jobs if needed.  None of these are available for Spark yarn-cluster mode.  Therefore we have to do something about it.


![Image of Yarn Mode](https://github.com/chesterxgchen/posts/blob/master/spark_integration/images/spark-yarn-mode.png)

 
## Runtime Jar file dependencies

Before we submit Spark, we need to talk about the runtime jar dependencies. As we discussed earlier, Spark may need additional dependencies jars files for the Spark job. This could be 3rd party dependencies jar by Spark or by your applications. You could use let Spark upload for you by specify “--files” in arguments. The “—files” option will upload the files and distributed the file to all the nodes in the cluster. One thing to know that the jar file uploaded with “--files” option is persisted in HDFS under Yarn Job Application Id directory, therefore cannot be reused for another job.
 
To avoid repeated upload the same jar, we upload the jar files ourselves; and only the files that have different timestamps and (md5) checksums are re-loaded. This way, the common jar files can be reused by all the Spark jobs.
 
If the cluster already has the spark-assembly.jar installed, we simply use it if it is with the match Spark version. Otherwise we upload the Spark-assembly.jar file. This also allows us to experiments with different Spark versions.  Once the jar is loaded, the HDFS file path can be used as arguments for the Spark job.


## Submit Spark Job
 
The standard way to submit a Spark job is to use spark-submit shell.  Unfortunately, the spark-submit (which internally calls SparkSubmit.scala) is mainly designed for shell invocation.
 
Spark recently introduced the SparkLauncher with a Builder Pattern and allows one to easily build required arguments. SparkLauncher.launch() calls “spark-submit” command line and started a separate process for spark job.  Although this makes it easier to launch Spark Job using SparkLauncher, SparkLauncher still doesn’t address our needs.
 
Our Approach:
 
*  Since we only submit theSpark job in Yarn-Cluster Mode, we will directly call Spark Yarn Client instead of going SparkSubmit or SparkLauncher, which still end up calling Spark Yarn Client.
* By calling Spark Yarn Client directly, we can control other interaction with Yarn. The Spark launcher will only return the Process object for the “spark-submit” command. You can’t not kill the spark job or get progress from it.
 
To that, first we create a new SparkYarnClient class as a customized Spark Yarn Client. It overwrites some of the behaviors defined in the Client.scala class in yarn module.

The Spark Client.scala is a private [spark] class and not intended to overwrite. Therefore we have to create the SparkYarnClient class in the same org.apache.spark.deploy.yarn package. To launch the spark job, we just need to call SparkYarnClient.run()
 
 
Since we have the SparkYarnClient reference, we can now
*  Kill the yarn application (Spark job) when "stop" command is called. The client.stop doesn't stop it immediately and Client.scala doesn't provide such functionality.
* Add YarnApplicationListener, which can be used to monitoring the Yarn Lifecycle.
* By default, the spark yarn ```Client.run()``` is a blocking call. It periodically pull yarn report to monitor the progress. With YarnSparkClient, we can call back to the listeners to notify the state change in Yarn. The notification can be done via overwrite the ```monitorApplication()``` method
 
Here SparkYarnClient is defined as following
```
object SparkYarnClient {

  def apply(args: Array[String], hadoopConf: Configuration, sparkConf: SparkConf) = {
  	    val clientArgs = new ClientArguments(args,sparkConf)
 	    new SparkYarnClient(clientArgs,hadoopConf, sparkConf)
  }
 
  //other methods
}
 
sealed class SparkYarnClient(override val args: ClientArguments,
          	             override val hadoopConf: Configuration,
             	             override val sparkConf: SparkConf) extends Client(args,hadoopConf,sparkConf) {
 
   //rest of code
}
``` 
One can invoke SparkYarnClient via

``` 
val sparkClient = SparkYarnClient(args, hadoopConf, sparkConf)
 
sparkClient.run()
``` 
 
## Add Yarn Application Listener
 
The first change we made is to add Yarn Application Listener to Spark Yarn Client, so that when Spark jobs starts, we can get the yarn callback, we can monitor the yarn progress.  The call back also provides the Application Id, which can then be used to kill the application.

``` 
private val listeners = ListBuffer[YarnApplicationListener]()

def addApplicationListener(listener: YarnApplicationListener) {
  listeners += listener
}

private def notifyAppStart(report: ApplicationReport) {
  val appInfo: YarnAppInfo = getApplicationInfo(report)
  listeners.par.foreach(_.onApplicationStart(appInfo.startTime, appInfo))
}

```
 
## Add KillApplication() method to Client
 
Many times we like to stop the yarn application before it finishes (debugging, experimenting and many other reasons). The Spark Client has the stop() method, which simply calls ```Client.stop()```. This doesn’t seem to stop the yarn application. We need to expose YarnClient’s ```killApplication()``` method.
 
Since Spark Client’s yarnClient field is private, we have to use reflection to access it.
``` 
def killApplication(appId: ApplicationId ) = {
   //use reflection to call yarnClient.killApplication(appId)
}
```

 

## Notify the yarn application state changes
 
The Spark has runAndForget configuration to control the run() method.  If the runAndForget = true, the Client does not monitor the application state. It is asynchronous call. Once the job is submitted, the client can do other tasks. By default, the runAndForget = false, so the ```Client.run()``` is a blocking call, the method returns when job is completed. During the run, the monitoring method periodically poll yarn application report, and log the report if logging is enabled.
 
For the case runAndForget = false, we like to notify yarn application listeners every time the state is reported. To do so, we overwrite the ```Client.monitor()``` method to allows us to inject the notify methods.

```

override
def monitorApplication(appId: ApplicationId,
                       returnOnRunning: Boolean = false,
                       logApplicationReport: Boolean = true): (YarnApplicationState, FinalApplicationStatus) = {

  val (report0, state0, status0)  = getApplicationReportWithState(appId)
  if (report0 == null) return (state0, status0)
  notifyAppStart(report0)

  val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)
  var lastState: YarnApplicationState = null
  while (true) {
	Thread.sleep(interval)
	val (report, state, status)  = getApplicationReportWithState(appId)
	if (report == null) return (state, status)

	notifyAppState(state, report)

	//rest of the codes
	// …
  }

  // Never reached, but keeps compiler happy
  throw new SparkException("While loop is depleted! This should never happen...")
}
 
```
 
Here are the different stages of life cycle notifications:

```
trait YarnApplicationListener {
  def onApplicationInit(time:Long, appId: ApplicationId)
  def onApplicationStart(time:Long, info: YarnAppInfo)
  def onApplicationProgress(time:Long, progress: YarnAppProgress)
  def onApplicationEnd(time:Long, progress: YarnAppProgress)
  def onApplicationFailed(time:Long, progress: YarnAppProgress)
  def onApplicationKilled(time:Long, progress: YarnAppProgress)

}
 
override def onApplicationStart(time: Long, info: YarnAppInfo) {
  logger.info(s"onApplicationStart time =$time, info=$info")
  jobState.updateAppStats(SPARK_YARN_APP_ID, info.appId)

  val modifiedUrl = if (MessagingUtil.isValidUrl(info.trackingUrl)) info.trackingUrl else ""
  val modifiedInfo = info.copy(trackingUrl = modifiedUrl)
  jobState.updateAppStats(SPARK_APP_INFO, modifiedInfo)
  val taskMessage = StartTaskMessage(info.appId.toString, "yarn app started", time) //message is not used.
  MessagingUtil.notifyJobStartWithTrackUrl(processListeners, nodeName, taskMessage, modifiedUrl)

}

```
 
Let’s look at the ```onApplicationStart()``` Callback:
* We updated the SPARK_YARA_APP_ID, with the info.appId, this is the yarn application Id just created. With this Application Id, we can use it to kill Application
* The job state is updated with the tracking URL
* Further the ```StartTaskMessage()``` is send to the front-end via a ```MessagingUtil```. 
 
This allows the application to display Yarn Progress Bar before Spark Job is started.

![image of yarnstart] (https://github.com/chesterxgchen/posts/blob/master/spark_integration/images/realtime_console_yarn_start.png) 
  
 
## Setup communication Channel
![image of communication chanel] (https://github.com/chesterxgchen/posts/blob/master/spark_integration/images/communication.png)
 
We also need a way for the Spark job to communicate back to the application on the logging, exception, as well as Spark progress.  

To do this, we setup a Spark Client Listener
 
SparkClientListener listens the messages sent back from Spark Job. The message is sent back via Akka messages.
 
Before we submit the Spark job, we create a SparkClientListener Actor and send the Actor’s URL to Spark job as part of the arguments.  Here is the method to submit the Spark Job (omitting a few lines of code)
 
One the client side:
```
def submitJob(conf: mutable.Map[String, String], hadoopConfig: Configuration) : Unit =  {
 try {
  	/* code omitted */

  	val (args, sparkConf)  = createYarnClientArgs(conf, hadoopConfig)
  	val sparkClient = SparkYarnClient(args, hadoopConf, sparkConf)

  	sparkClientListener = createSparkClientListener( /*…*/ )
 	
 	 sparkClient.addApplicationListener(new YarnAppListener(/* …*/) )

  	checkAndUploadJars(supportedSparkVersion)

  	logger.info("\n\n" + conf.mkString("\n"))

  	sparkClient.run()
  } finally {
	postRun(jobState)
  }
 
```  
On the cluster
 
All Spark job implements a SparkMain() method as we wrap the Spark job in the followings:

``` 
abstract class AlpineSparkJob {

def main(args: Array[String]) {

  var appContext: Option[ApplicationContext] = None
  try {
     val conf : mutable.Map[String, String] = deSerializeConf()
 	val appCtx = new ApplicationContext(conf)
	
 	/* … Omit code … */

	 logger = appCtx.logger
 
	sparkMain(appCtx)
 
  } catch { /* … */}
	finally {/* … */}
 
}

``` 
 
For all Spark jobs, we create an ApplicationContext, which contains the all the needed infrastructure to run the Spark Job.
```
class ApplicationContext(val conf: mutable.Map[String, String]) {

  val appName = conf.getOrElse(SPARK_APP_NAME, "Alpine Spark Job")
  val taskChannelActorSystem : ActorSystem = createTaskChannelActorSystem
  val messenger: Option[ActorRef] = createServerMessenger()

  val sparkCtx: SparkContext = createSparkContext(conf)

  val logger = new MessageLogger(appName, Some(this))
 
  //add spark listener
  sparkCtx.addSparkListener(new JobProgressRelayListener(this))

  showConf()
 
   /*… rest of methods … */
}
``` 
 
 
First we create an Akka Actor System in the container so that it can be used to send and receive akka messages.  With this Akka Actor System, we created messenger, logger and JobProgressRelayListener. Note that SparkContext also contains an actor system, but it is deprecated and should be not used
 
 
Messenger – is used to send messages via established task communication channel. The messenger can send all types of messages
 
Logger  -- only handle log messages, it uses messenger to send log related messages.
 
JobProgressRelayListener – is a SparkListener, it uses messenger to relay Spark job Progresses to the Client Side (such as log or UI)
 
With this infrastructure, we can do a lot of interesting communications from cluster to client.
 
 
Let’s take a closer look at the type of messages we sending from cluster to client:
 
## Message Types
```

trait LogMessage {
  val time: Long
  val name: String
  val message: String
  override def toString: String = s"[$time-$name]:$message"
}
 
trait UIMessage {
  val time: Long
  val task: String
  val message: String
}
 


trait AppMessage {
  val name: String
  val key: String
  val value: Any //message needs to be serializable
}
``` 
The messages are divided into three categories: LogMessage, UIMessage, and AppMessage.   LogMessage will be used for logging, UIMessage will be used for display and AppMessage message to communicate to application.
 
Here are the concrete messages corresponding to log Levels (Info, Warn, Debug and Error)
```
case class InfoMessage(message: String, name: String, time: Long = new Date().getTime) extends LogMessage
case class WarnMessage(message: String, name: String, time: Long = new Date().getTime) extends LogMessage
case class DebugMessage(message: String, name: String, time: Long = new Date().getTime) extends LogMessage
case class ErrorMessage(message: String, name: String, cause: Throwable, time: Long = new Date().getTime) extends LogMessage
```
 
Here are the concrete messages used for UI. ProgressBarMessage is used for display progress bar, DisplayMessage will just show text on the UI.
```
case class ProgressBarMessage(task: String, message: String, progressMessage: String, time: Long = new Date().getTime) extends UIMessage

case class DisplayMessage(task: String, message: String, time: Long = new Date().getTime) extends UIMessage
```

Here is one UpdateMessage, where we use to update key,values of the application.
```
case class UpdateMessage(name: String, key: String, value: Any) extends AppMessage

case class BroadcastMessage(jobType: String, name: String, key: String, value: Any) extends AppMessage
```
The BroadcastMessage is used for send messages for all the Machine Learning Listeners, this will be discussed in a separate post. 
 

 
## Spark Client Listener
 
Spark Client Listener resides on the client side of the application (not in cluster) and it is used for receiving messages from the Spark job. Once the message is received, it redirect to different destination based on the type of the messages:
 
* LogMessages => Log4J to log
* UIMessage => send to message queue for Websocket to pick and update UI, displaying real-time progress and errors
* AppMessages => directly update application states, counters or broadcast the message to part of the application. 
 

 
## Displaying Spark Job Progress in Real-Time 
 
With above infrastructure, now we are ready to display real-time Spark job progress and other detailed information to UI.  What we need to do is first get the Spark job progress via JobProgressListener and then relay the progress via our task communication channel to front-end.  Here we define a Spark JobProgressRelayListener extends JobProgressListener to do this work.
![image of relaylistener](https://github.com/chesterxgchen/posts/blob/master/spark_integration/images/spark-relay-listener.png)

``` 
class JobProgressRelayListener(appCtx: ApplicationContext)
	extends JobProgressListener(appCtx.sparkCtx.getConf) with SparkListener {

  private val SHOW_STAGE_PROGRESS_KEY =  "alpine.spark.show.stage.progress"
  private val logger = new MessageLogger(appCtx.appName, Some(appCtx))
  private val sc = appCtx.sparkCtx
  private implicit val sparkCtx = Some(sc)
  private implicit val messenger = appCtx.messenger
  private val showStageProgress: Boolean = appCtx.conf.getOrElse(SHOW_STAGE_PROGRESS_KEY, "true").toBoolean

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
	if (showStageProgress) {
      	    super.onStageCompleted(stageCompleted)
  	…
  	   val progressMessage: Option[ProgressBarMessage] =  { /* …code omitted … */}
  	   progressMessage.foreach(logger.sendUIMessage)
	}
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
	super.onApplicationStart(applicationStart)
	val taskMessage = StartTaskMessage(applicationStart.appName, "started", applicationStart.time)
	logger.sendUIMessage(taskMessage)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
	super.onApplicationEnd(applicationEnd)
	val taskMessage = EndTaskMessage(appCtx.appName, "end", applicationEnd.time)
	logger.sendUIMessage(taskMessage)
   }

}
``` 


Now with this infrastructure in place, we can display in real-time, the Spark progress. 
 



 
 
## Summary
 
I have described in great detail about the alpine Spark Integration approach. Hope this is helpful for you work as well.
 
We are continue to working on additional infrastructure related to Spark Integration:
 
* Real-Time Machine Learning visualization with Spark
   Enable one to visualize and monitor the machine training at each iteration. The intermediate training result is sent to front-end via    established communication channel and displayed as training in progress.
* Interactive with running spark Job
  The task channel actor can be created to take command to change the behavior of the spark job. For example, One can stop the macine   learning training iteration instead of waiting for the last iteration if user determine that the convergence is achieved.
 ( check out my presentations at 2016 IEEE International Conference on Big Data Analytics and other conferences )

* Automatically estimate the Spark configuration parameters based on the yarn containers memory, vCores, data size
 

