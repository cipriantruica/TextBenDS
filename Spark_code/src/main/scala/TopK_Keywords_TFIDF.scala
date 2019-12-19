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


object TopK_Keywords_TFIDF {
  def main(args: Array[String]): Unit = {
    val noTweets = args(0).toInt
    val query = args(1).toInt // 1, 2, 3, 4
    val genderType = args(2).toInt // 0 - female, 1 male
    val noTest = args(3)
    val schemaName = "TwitterDB" + noTweets + "K"
    val gender = Vector("female", "male")
    val startDate = "cast('2015-09-17 00:00:00' as timestamp)"
    val endDate   = "cast('2015-09-18 00:00:00' as timestamp)"
    val top = 10
    val startX = 20
    val endX   = 40
    val startY = -100
    val endY   = 100
    // Spark session
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Top-K Keywords TFIDF")

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    hc.table(schemaName + ".author_dimension").createOrReplaceTempView("author_dimension")
    hc.table(schemaName + ".document_dimension").createOrReplaceTempView("document_dimension")
    hc.table(schemaName + ".document_facts").createOrReplaceTempView("document_facts")
    hc.table(schemaName + ".location_dimension").createOrReplaceTempView("location_dimension")
    hc.table(schemaName + ".time_dimension").createOrReplaceTempView("time_dimension")
    hc.table(schemaName + ".word_dimension").createOrReplaceTempView("word_dimension")

    var q = ""
    if (query == 1){
      q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender(genderType) + "' group by f.id_document ), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender(genderType) + "' group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender(genderType) + "' group by wd.word order by 2 desc limit " + top
    }
    else if (query == 2) {
      q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender(genderType) + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender(genderType) + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word inner join time_dimension td on td.id_time = f.id_time cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender(genderType) + "' and td.full_date between " + startDate + " and " + endDate + " group by wd.word order by 2 desc limit " + top
    }
    else if (query == 3) {
      q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word inner join location_dimension ld on ld.id_location = f.id_location cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by wd.word order by 2 desc limit " + top
    }
    else if (query == 4) {
      q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by f.id_document ), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender(genderType) + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by wd.word order by 2 desc limit " + top
    }

    val t0 = System.nanoTime()
    hc.sql(q).show()
    val t1 = System.nanoTime()
    println(schemaName + "_q" + query + "_" + gender(genderType) + "_TFIDF_" + noTest + " : " + ((t1 - t0) / 1e6) + " (ms)")

  }
}