package org.bigdata

/**
 * 主入口
 * 工作流协调器
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("用法: spark-submit --class org.bigdata.Main <task>")
      println("可用任务:")
      println("  help    - 显示帮助信息")
      return
    }

    args(0) match {
      case "help" => printHelp()
      case _ => println(s"未知任务: ${args(0)}")
    }
  }

  def printHelp(): Unit = {
    println("SparkMain 工作流")
    println("================")
    println("")
    println("个人任务请使用个人分支运行")
    println("示例:")
    println("  git checkout yiyangchen609-web")
    println("  spark-submit --class org.bigdata.analysis.AnalyzeRatings <jar>")
  }
}
