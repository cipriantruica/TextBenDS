import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Copyright (C) 2018 Ciprian-Octavian Truică <ciprian.truica@cs.pub.ro>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
    It reads all the data from the tcv and computes the edges.
    It uses a Dataframe with a single select.
    It saves the output in Hive
*/

object CreateDatabase {
  def main(args: Array[String]): Unit = {
    // input directory with the tsv
    val noTweets = args(0)
    val path = args(1) // e.g. hdfs://IP:PORT/path_to_file/
    val schemaName = "TwitterDB" + noTweets + "K"
    val authorDimensionFile = path + schemaName + "/author_dimension.csv"
    val documentDimensionFile = path + schemaName + "/document_dimension.csv"
    val documentFactsFile = path + schemaName + "/document_facts.csv"
    val locationDimensionFile = path + schemaName + "/location_dimension.csv"
    val timeDimensionFile = path + schemaName + "/time_dimension.csv"
    val wordDimensionFile = path + schemaName + "/word_dimension.csv"
    val bridgeTagDimensionFile = path + schemaName + "/bridge_tag_dimension.csv"
    val tagDimensionFile = path + schemaName + "/tag_dimension.csv"
    val bridgeNamedEntityDimensionFile = path + schemaName + "/bridge_namedentity_dimension.csv"
    val namedEntityDimensionFile = path + schemaName + "/namedentity_dimension.csv"

    // the tsv schema
    val authorDimensionSchema = StructType(Array(
      StructField("id_author", LongType, false),
      StructField("firstname", StringType, true),
      StructField("lastname", StringType, true),
      StructField("age", IntegerType, true),
      StructField("gender", StringType, true)))

    val documentDimensionSchema = StructType(Array(
      StructField("id_document", LongType, false),
      StructField("raw_text", StringType, true),
      StructField("lemma_text", StringType, true),
      StructField("clean_text", StringType, true)))

    val documentFactsSchema = StructType(Array(
      StructField("id_document", LongType, false),
      StructField("id_word", LongType, false),
      StructField("id_location", LongType, false),
      StructField("id_author", LongType, false),
      StructField("id_time", LongType, false),
      StructField("count", DoubleType, true),
      StructField("tf", DoubleType, true)))

    val locationDimensionSchema = StructType(Array(
      StructField("id_location", LongType, false),
      StructField("x", DoubleType, true),
      StructField("y", DoubleType, true)))

    val timeDimensionSchema = StructType(Array(
      StructField("id_time", LongType, false),
      StructField("second", IntegerType, true),
      StructField("minute", IntegerType, true),
      StructField("hour", IntegerType, true),
      StructField("day", IntegerType, true),
      StructField("month", IntegerType, true),
      StructField("year", IntegerType, true),
      StructField("full_date", TimestampType, true)))

    val wordDimensionSchema = StructType(Array(
      StructField("id_word", LongType, false),
      StructField("word", StringType, true)))

    val bridgeTagDimension = StructType(Array(
      StructField("id_document", LongType, false),
      StructField("id_tag", LongType, false)))

    val tagDimension = StructType(Array(
      StructField("id_tag", LongType, false),
      StructField("tag", StringType, true),
      StructField("type", StringType, true)))

    val bridgeNamedEntityDimension = StructType(Array(
      StructField("id_document", LongType, false),
      StructField("id_namedentity", LongType, false)))

    val namedEntityDimension = StructType(Array(
      StructField("id_namedentity", LongType, false),
      StructField("entity", StringType, true),
      StructField("type", StringType, true)))


    // Spark session
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Create Database TwitterDB" + noTweets + "K")

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop tables if it exists
    hc.sql("drop table if exists " + schemaName + ".author_dimension")
    hc.sql("drop table if exists " + schemaName + ".document_dimension")
    hc.sql("drop table if exists " + schemaName + ".document_facts")
    hc.sql("drop table if exists " + schemaName + ".location_dimension")
    hc.sql("drop table if exists " + schemaName + ".time_dimension")
    hc.sql("drop table if exists " + schemaName + ".word_dimension")
    hc.sql("drop table if exists " + schemaName + ".bridge_tag_dimension")
    hc.sql("drop table if exists " + schemaName + ".tag_dimension")
    hc.sql("drop table if exists " + schemaName + ".bridge_namedentity_dimension")
    hc.sql("drop table if exists " + schemaName + ".namedentity_dimension")

    val t0 = System.nanoTime()

    // read the data from the tsv files
    val authorDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(authorDimensionSchema).load(authorDimensionFile)
    val documentDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(documentDimensionSchema).load(documentDimensionFile)
    val documentFactsDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(documentFactsSchema).load(documentFactsFile)
    val locationDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(locationDimensionSchema).load(locationDimensionFile)
    val timeDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(timeDimensionSchema).load(timeDimensionFile)
    val wordDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(wordDimensionSchema).load(wordDimensionFile)
    val bridgeTagDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(wordDimensionSchema).load(bridgeTagDimensionFile)
    val tagDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(wordDimensionSchema).load(tagDimensionFile)
    val bridgeNamedEntityDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(wordDimensionSchema).load(bridgeNamedEntityDimensionFile)
    val namedEntityDimensionDF = hc.read.format("csv").option("header", "false").option("delimiter", ",").schema(wordDimensionSchema).load(namedEntityDimensionFile)

    // create a view to query
    authorDimensionDF.createOrReplaceTempView("author_dimension")
    hc.sql("select * from author_dimension").write.format("orc").saveAsTable(schemaName + ".author_dimension")

    documentDimensionDF.createOrReplaceTempView("document_dimension")
    hc.sql("select * from document_dimension").write.format("orc").saveAsTable(schemaName + ".document_dimension")

    documentFactsDF.createOrReplaceTempView("document_facts")
    hc.sql("select * from document_facts").write.format("orc").saveAsTable(schemaName + ".document_facts")

    locationDimensionDF.createOrReplaceTempView("location_dimension")
    hc.sql("select * from location_dimension").write.format("orc").saveAsTable(schemaName + ".location_dimension")

    timeDimensionDF.createOrReplaceTempView("time_dimension")
    hc.sql("select * from time_dimension").write.format("orc").saveAsTable(schemaName + ".time_dimension")

    wordDimensionDF.createOrReplaceTempView("word_dimension")
    hc.sql("select * from word_dimension").write.format("orc").saveAsTable(schemaName + ".word_dimension")

    bridgeTagDimensionDF.createOrReplaceTempView("bridge_tag_dimension")
    hc.sql("select * from bridge_tag_dimension").write.format("orc").saveAsTable(schemaName + ".bridge_tag_dimension")

    tagDimensionDF.createOrReplaceTempView("tag_dimension")
    hc.sql("select * from tag_dimension").write.format("orc").saveAsTable(schemaName + ".tag_dimension")

    bridgeNamedEntityDimensionDF.createOrReplaceTempView("bridge_namedentity_dimension")
    hc.sql("select * from bridge_namedentity_dimension").write.format("orc").saveAsTable(schemaName + ".bridge_namedentity_dimension")

    namedEntityDimensionDF.createOrReplaceTempView("namedentity_dimension")
    hc.sql("select * from namedentity_dimension").write.format("orc").saveAsTable(schemaName + ".namedentity_dimension")

    val t1 = System.nanoTime()
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))

  }
}
