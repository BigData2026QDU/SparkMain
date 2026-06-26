package org.bigdata

import org.bigdata.streaming.RatingStreamProcessor
import org.bigdata.analysis.{AnalyzeRatings, AnalyzeGenres, AnalyzeTime, AnalyzeUsers}

/**
 * 主入口
 * 根据参数执行不同的任务
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("用法: spark-submit --class org.bigdata.Main <task>")
      println("可用任务:")
      println("  stream    - 启动评分数据流处理")
      println("  analyze   - 运行评分分析")
      println("  genres    - 运行类型分析")
      println("  time      - 运行时间分析")
      println("  users     - 运行用户行为分析")
      return
    }

    args(0) match {
      case "stream" => RatingStreamProcessor.main(args.drop(1))
      case "analyze" => AnalyzeRatings.main(args.drop(1))
      case "genres" => AnalyzeGenres.main(args.drop(1))
      case "time" => AnalyzeTime.main(args.drop(1))
      case "users" => AnalyzeUsers.main(args.drop(1))
      case _ => println(s"未知任务: ${args(0)}")
    }
  }
}
