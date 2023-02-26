package com.at.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-16 15:34
 */
object Spark05_task {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    //聚合
    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    // Job：一个Action算子就会生成一个Job；
    //job1打印到控制台
    resultRDD.collect().foreach(println)

    //job2输出到磁盘
    resultRDD.saveAsTextFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\output")

    Thread.sleep(10000000)
    // 关闭连接
    sc.stop()
  }

  /*
    SparkContext$runJob
      ⬇
    DAGScheduler$runJob
      val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
      ⬇
    DAGScheduler$submitJob
      eventProcessLoop.post(...)
      ⬇
    EventLoop$eventThread
      onReceive(event)
      ⬇
    DAGSchedulerEventProcessLoop$doOnReceive
       JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
          dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
       ⬇
    DAGScheduler$handleJobSubmitted
        1.finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
        2.job与stage关系 val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
          stage与job关系 finalStage.setActiveJob(job)
        3.submitStage(finalStage)

    1.finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
        DAGScheduler$createResultStage
           val parents = getOrCreateParentStages(rdd, jobId)
           一上来直接创建了一个Stage
           val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
          ⬇
        DAGScheduler$getOrCreateParentStages
           getShuffleDependencies(rdd).map { shuffleDep =>
              getOrCreateShuffleMapStage(shuffleDep, firstJobId)
           }.toList
          ⬇
        DAGScheduler
            $getShuffleDependencies 只会获取rdd最近的一个宽依赖
               toVisit.dependencies.foreach {
                  case shuffleDep: ShuffleDependency[_, _, _] => parents += shuffleDep
                  case dependency => waitingForVisit.push(dependency.rdd)
                }

            $getOrCreateShuffleMapStage
                获取其他宽依赖
                getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
                   if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
                    创建Stage
                    createShuffleMapStage(dep, firstJobId)}
                }
                创建Stage
                createShuffleMapStage(shuffleDep, firstJobId)

             $getMissingAncestorShuffleDependencies 查找其余的宽依赖
                  getShuffleDependencies(toVisit).foreach { shuffleDep =>
                      if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
                        ancestors.push(shuffleDep)
                        waitingForVisit.push(shuffleDep.rdd)}}

             $createShuffleMapStage
                val rdd = shuffleDep.rdd 创建ShuffleMapStage的RDD是阶段最后的RDD
                val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)
         一个宽依赖会创建一个Stage

     3.submitStage(finalStage)
        DAGScheduler
            $submitStage
              submitMissingTasks(stage, jobId.get)
            $submitMissingTasks
              val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
              val tasks: Seq[Task[_]] = try {
                stage match {
                  case stage: ShuffleMapStage =>
                    //分区数计算(val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()) 然后遍历 每一个分区创建一个Task
                    partitionsToCompute.map { id =>
                      ...
                      new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,taskBinary, part, locs,
                        stage.latestInfo.taskMetrics, properties, Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
                    }
         Stage(ShuffleMapStage/ResultStage)$findMissingPartitions
         ShuffleMapStage$findMissingPartitions
             override def findMissingPartitions(): Seq[Int] = {
                val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
                ...
                missing
             }
             Stage
              val numPartitions = rdd.partitions.length (rdd为创建Stage时传过来的)
              所以每一个阶段最后一个RDD的分区数，就是当前阶段的Task个数


   */


  /*
    Spark的Job调度
      -集群(Standalone|Yarn)
        *一个Spark集群可以同时运行多个Spark应用

      -应用
        *我们所编写的完成某些功能的程序
        *一个应用可以并发的运行多个Job

      -Job
        *Job对应着我们应用中的行动算子，每次执行一个行动算子，都会提交一个Job
        *一个Job由多个Stage组成

      -Stage
        *一个宽依赖做一次阶段的划分(对于窄依赖，partition的转换处理在Stage中完成计算。对于宽依赖，由于有Shuffle的存在，只能在parent RDD处理完成后，才能开始接下来的计算，因此宽依赖是划分Stage的依据)
        *阶段的个数 =  宽依赖个数  + 1
        *一个Stage由多个Task组成

      -Task
        *每一个阶段最后一个RDD的分区数，就是当前阶段的Task个数


   */


}
