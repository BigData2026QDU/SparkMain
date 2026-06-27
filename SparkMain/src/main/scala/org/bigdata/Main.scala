package org.bigdata

import org.bigdata.streaming.{RealtimeWorkflow, UserBehaviorRealtimeJob, UserBehaviorReplayProducer}

/**
 * 主入口
 * 工作流协调器
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("用法: spark-submit --class org.bigdata.Main <task>")
      println("可用任务:")
      println("  realtime  - 启动实时数据处理（Kafka → MySQL）")
      println("  user-realtime - 启动淘宝用户行为 5 分钟实时分析")
      println("  user-replay   - 回放淘宝历史用户行为日志到 Kafka")
      println("  help      - 显示帮助信息")
      return
    }

    args(0) match {
      case "realtime" => RealtimeWorkflow.main(args.drop(1))
      case "user-realtime" => UserBehaviorRealtimeJob.main(args.drop(1))
      case "user-replay" => UserBehaviorReplayProducer.main(args.drop(1))
      case "help" => printHelp()
      case _ => println(s"未知任务: ${args(0)}")
    }
  }

  def printHelp(): Unit = {
    println("SparkMain 工作流")
    println("================")
    println("")
    println("实时数据处理：")
    println("  ./main_pipeline_new.sh stream")
    println("  ./main_pipeline_new.sh user-stream")
    println("")
    println("个人任务请使用个人分支：")
    println("  git checkout yiyangchen609-web")
  }
}
