package org.bigdata

import org.bigdata.analysis.{AnalyzeGenres, AnalyzeRatings, AnalyzeTime, AnalyzeUsers}

/**
 * Personal analysis task entry point.
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: spark-submit --class org.bigdata.Main <task>")
      println("Available tasks:")
      println("  analyze   - run movie rating statistics")
      println("  genres    - run genre rating statistics")
      println("  time      - run time-based rating statistics")
      println("  users     - run user behavior analysis")
      return
    }

    args(0) match {
      case "analyze" => AnalyzeRatings.main(args.drop(1))
      case "genres" => AnalyzeGenres.main(args.drop(1))
      case "time" => AnalyzeTime.main(args.drop(1))
      case "users" => AnalyzeUsers.main(args.drop(1))
      case _ => println(s"Unknown task: ${args(0)}")
    }
  }
}
