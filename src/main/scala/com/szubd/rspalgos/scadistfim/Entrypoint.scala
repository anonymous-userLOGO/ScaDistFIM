package com.szubd.rspalgos.scadistfim
import java.util.Calendar
import java.text.SimpleDateFormat


import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.{Row, RspDataset, SparkSession}
import org.apache.spark.sql.RspContext._
import com.szubd.rspalgos.scadistfim.ScaDistFim
import org.apache.spark.sql.functions.{col,size}
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}

/**
 * usage: fim [spark|scaDist] [OPTIONS] [source-file] [min-support] [partitions...]
 * Run FPGrowth
 * -help                    Show help.
 * -o,--output-head <arg>   result output file head.
 *
 * examples:
 *   run FPGrowth using spark mllib:
 *     fim spark Sultan_Itemsets_64M.parquet 0.001 5 10
 *   run FPGrowth using logo mllib scaDist:
 *     fim scaDist Sultan_Itemsets_64M.parquet 0.001 5 10
 */
object Entrypoint {


  lazy val HelpOption: Option = {
    val help = new Option("help", false, "Show help.")
    help.setRequired(false)
    help
  }

  val FimUsage = "[spark|scaDist] [OPTIONS] [source-file] [min-support] [partitions...]"
  val FimHeader = "Run FPGrowth"
  lazy val FimOptions: Options = {
    val options = new Options()

    val output = new Option(
      "o", "output-head", true, "result output file head."
    )
    options.addOption(output)
//    val outputDir = new Option(
//      "d", "output-dir", true, "output file dir."
//    )
//    options.addOption(outputDir)
    options.addOption(HelpOption)
    options
  }

  class Params(
              sourceFile: String,
              outputHead: String,
              minSupport: Double,
              blockSizes: Array[Int]
              ){
    val SourceFile: String = sourceFile
    val OutputHead: String = outputHead
    val MinSupport: Double = minSupport
    val BlockSizes: Array[Int] = blockSizes
  }

  def parse(args: Array[String]): Params = {
    val parser = new BasicParser()
    val cmd = parser.parse(FimOptions, args)
    val arguments = cmd.getArgs
    val sourceFile = arguments(0)
    val minSupport = arguments(1).toDouble
    val blockSizes = arguments.slice(2, arguments.length).map(_.toInt)
    var outputHead = ""
    if (cmd.hasOption("o")) {
      outputHead = cmd.getOptionValue("o")
    }else{
      val dt = Calendar.getInstance()
      val form = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss")
      outputHead = "FimExperiments/FimOutput-%s-%s-%f".format(form.format(dt.getTime),sourceFile,minSupport)
    }
    new Params(sourceFile, outputHead, minSupport, blockSizes)
  }

  def onArgs(spark: SparkSession, args: Array[String]): Unit = {
    printf("onArgs: %s\n", args.reduce(_+" "+_))
    args(0) match {
//      case "spark" => sparkFPG(spark, args(1), args(2).toDouble, args.slice(3, args.length).map(_.toInt))
      case "spark" => sparkFPG(spark, parse(args.slice(1, args.length)))
//      case "scaDist" => scaDistFim(spark, args(1), args(2).toDouble, args.slice(3, args.length).map(_.toInt))
      case "scaDist" => scaDistFim(spark, parse(args.slice(1, args.length)))
      case "help" => {
        val help = new HelpFormatter()
        help.printHelp(FimUsage, FimHeader, FimOptions, "")
      }
      case _ => printf("Unknown type: %s\n", args(0))

    }
  }

//  def sparkFPG(session: SparkSession, sourceFile: String, minSupport: Double, blockSizes: Array[Int]): Unit = {
def sparkFPG(session: SparkSession, params: Params): Unit = {
    val df = session.rspRead.parquet(params.SourceFile)
    for (size <- params.BlockSizes){
      printf("size=%d\n", size)
      var rdf = df.getSubDataset(size)
      val fpg = new FPGrowth().setItemsCol("items").setMinSupport(params.MinSupport).setNumPartitions(100)//0.3
      val model = fpg.fit(rdf)
      val count = rdf.count()
      printf("FIM count = %d\n", model.freqItemsets.count)
      val mdf = model.freqItemsets
      val sdf = mdf
        .withColumn("frequency", mdf.col("freq")/count.toDouble)
        .withColumnRenamed("freq", "support")
      sdf.show()
//      val outputFile = "%s_%dBlock.parquet".format(params.OutputHead, size)
      val outputFile = "%s_%dBlock_Spark.parquet".format(params.OutputHead, size)
      writeResultSpark(outputFile, sdf)
    }

  }

//  def scaDistFim(session: SparkSession, sourceFile: String, minSupport: Double, blockSizes: Array[Int]): Unit = {
  def scaDistFim(session: SparkSession, params: Params): Unit = {
    import session.implicits._
    val df = session.rspRead.parquet(params.SourceFile)
    for (size <- params.BlockSizes){
      printf("size=%d\n", size)
      val rdd = ScaDistFim.etl(df.getSubDataset(size))
      val items = ScaDistFim.run(rdd, params.MinSupport)
      val count = rdd.count()
      val idf = items.map(f => (f._1, f._2, f._2.toDouble/count)).filter(h => h._3 >= 0.01).toDF("items", "support", "frequency")
      idf.show()
      printf("FIM count = %d \n", idf.count())
      val outputFile = "%s_%dBlock_ScaDist.parquet".format(params.OutputHead, size)
      writeResultLogo(outputFile, idf)
    }
  }
  def calculateStatistics(df: DataFrame): DataFrame={
    val df1 = df.filter(size(col("items")) >= 2)
    printf("去除一项集后的结果数量 %d\n", df1.count())
    df1
  }
  def writeResultLogo(filename: String, df: DataFrame): Unit = {
    val df3 = calculateStatistics(df)
    df3.write.mode(SaveMode.Overwrite).parquet(filename)
    val df1 = df3.select(df3.col("items").cast("String"),df3.col("support"),df3.col("frequency"))
    df1.repartition(1).write.csv("/user/luokaijing/FimExperimentsCSV/fpgLogo_2600W_0.01.csv")
    printf("Write result %s\n", filename)
  }
  def writeResultSpark(filename: String, df: DataFrame): Unit = {
    val df3 = calculateStatistics(df)
    df3.write.mode(SaveMode.Overwrite).parquet(filename)
    val df1 = df3.select(df3.col("items").cast("String"),df3.col("support"),df3.col("frequency"))
    df1.repartition(1).write.csv("/user/luokaijing/FimExperimentsCSV/fpgSpark_275W_0.01.csv")
    printf("Write result %s\n", filename)
  }
}
