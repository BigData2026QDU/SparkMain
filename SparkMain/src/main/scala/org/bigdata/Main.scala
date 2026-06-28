package org.bigdata

import org.bigdata.analysis.{
  AnalyzeGenres,
  AnalyzeRatings,
  AnalyzeTime,
  AnalyzeUsers
}
import org.bigdata.streaming.RealtimeWorkflow

/**
 * 主入口
 * 工作流协调器
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      printHelp()
      return
    }

    args(0) match {
      case "ratings" | "analyze-ratings" | "AnalyzeRatings" =>
        AnalyzeRatings.main(args.drop(1))
      case "genres" | "analyze-genres" | "AnalyzeGenres" =>
        AnalyzeGenres.main(args.drop(1))
      case "time" | "analyze-time" | "AnalyzeTime" =>
        AnalyzeTime.main(args.drop(1))
      case "users" | "analyze-users" | "AnalyzeUsers" =>
        AnalyzeUsers.main(args.drop(1))
      case "realtime" => RealtimeWorkflow.main(args.drop(1))
      case "help" => printHelp()
      case _ =>
        println(s"未知任务: ${args(0)}")
        printHelp()
    }
  }

  def printHelp(): Unit = {
    println("SparkMain 工作流")
    println("================")
    println("")
    println("用法:")
    println("  spark-submit --class org.bigdata.Main <jar> <task> [inputDir] [outputDir]")
    println("")
    println("离线分析任务:")
    println("  ratings   - 电影评分质量报告（ratings + movies）")
    println("  genres    - 类型/类别热度报告（genres + ratings + movies）")
    println("  time      - 评分时间趋势报告（timestamps）")
    println("  users     - 用户行为分群报告（user behavior）")
    println("")
    println("实时统计任务:")
    println("  realtime  - 启动 Kafka Structured Streaming 评分统计")
    println("")
    println("帮助:")
    println("  help      - 显示帮助信息")
  }
}
