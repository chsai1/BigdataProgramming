package com.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object wordcountProgram {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input =  sc.textFile("input.txt")

    val output = "data/WordCountOutput"

    val words = input.flatMap(line => line.split("\\W+"))

    words.foreach(f=>println(f))

    val counts = words.map(words => (words, 1)).reduceByKey(_+_,1)

    val wordsList=counts.sortBy(outputLIst=>outputLIst._1,ascending = true)

    wordsList.foreach(outputLIst=>println(outputLIst))

    wordsList.saveAsTextFile(output)

    wordsList.take(10).foreach(outputLIst=>println(outputLIst))

    sc.stop()

  }

}