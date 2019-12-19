import java.io._

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


object CreateHiveQueries {
  def main(args: Array[String]): Unit = {
    val genders = Vector("female", "male")
    val startDate = "cast('2015-09-17 00:00:00' as timestamp)"
    val endDate = "cast('2015-09-18 00:00:00' as timestamp)"
    val top = 10
    val startX = 20
    val endX = 40
    val startY = -100
    val endY = 100
    val k1 = 1.6
    val b = 0.75

    val words = Vector("('think')", "('think','today')", "('think','today','friday')")

    // val queries = Vector(1, 2, 3, 4)
    // val noWords = Vector(1, 2, 3)


    System.out.println("TopK Keywords OkapiBM25")
    var q = ""
    for (query <- 1 to 4) {
      for (gender <- genders) {
        if (query == 1) {
          q = "with q_docLen as (select id_document, sum(count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_word) select wd.word, sum((1 + ln(noDocs.no/ndw.noDocWords)) * (" + k1 + " + 1) * f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/avgDocs.no))) Okapi from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select avg(docLen) no from q_docLen) avgDocs cross join (select count(id_document) no from q_docLen) noDocs where ad.gender = '" + gender + "' group by wd.word order by Okapi desc limit " + top + ";"
        }
        else if (query == 2) {
          q = "with q_docLen as (select id_document, sum(count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select wd.word, sum((1 + ln(noDocs.no/ndw.noDocWords)) * (" + k1 + " + 1) * f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/avgDocs.no))) Okapi from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join time_dimension td on td.id_time = f.id_time inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select avg(docLen) no from q_docLen) avgDocs cross join (select count(id_document) no from q_docLen) noDocs where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by wd.word order by Okapi desc limit " + top + ";"
        }
        else if (query == 3) {
          q = "with q_docLen as (select id_document, sum(count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select wd.word, sum((1 + ln(noDocs.no/ndw.noDocWords)) * (" + k1 + " + 1) * f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/avgDocs.no))) Okapi from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join location_dimension ld on ld.id_location = f.id_location inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select avg(docLen) no from q_docLen) avgDocs cross join (select count(id_document) no from q_docLen) noDocs where ad.gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by wd.word order by Okapi desc limit " + top + ";"
        }
        else if (query == 4) {
          q = "with q_docLen as (select id_document, sum(count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select wd.word, sum((1 + ln(noDocs.no/ndw.noDocWords)) * (" + k1 + " + 1) * f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/avgDocs.no))) Okapi from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_docLen dl on dl.id_document = f.id_document inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select avg(docLen) no from q_docLen) avgDocs cross join (select count(id_document) no from q_docLen) noDocs where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by wd.word order by Okapi desc limit " + top + ";"
        }
        System.out.println(q)
        val printFile  = "TopK_Keywords_Okapi_q" + query + "_" + gender + ".sql"
        val pw = new PrintWriter(new File(printFile))
        pw.println(q)
        pw.close()
      }
    }

    System.out.println("TopK Keywords TFIDF")
    for (query <- 1 to 4) {
      for (gender <- genders) {
        if (query == 1){
          q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_document ), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender + "' group by wd.word order by TFIDF desc limit " + top + ";"
        }
        else if (query == 2) {
          q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word inner join time_dimension td on td.id_time = f.id_time cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by wd.word order by TFIDF desc limit " + top + ";"
        }
        else if (query == 3) {
          q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join q_noDocWords ndw on ndw.id_word = f.id_word inner join location_dimension ld on ld.id_location = f.id_location cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by wd.word order by TFIDF desc limit " + top + ";"
        }
        else if (query == 4) {
          q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by f.id_document ), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select wd.word, sum(f.tf * (1+ln(dl.docLen/ndw.noDocWords))) TFIDF from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join word_dimension wd on wd.id_word = f.id_word inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) docLen from q_docLen) dl where ad.gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and td.full_date between " + startDate + " and " + endDate + " group by wd.word order by TFIDF desc limit " + top + ";"
        }
        System.out.println(q)
        val printFile  = "TopK_Keywords_TFIDF_q" + query + "_" + gender + ".sql"
        val pw = new PrintWriter(new File(printFile))
        pw.println(q)
        pw.close()
      }
    }

    System.out.println("TopK Documents Okapi BM25")
    for (query <- 1 to 4) {
      for (gender <- genders) {
        for (noWords <- 0 to 2) {
          if (query == 1) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_word ) select f.id_document, sum(( 1 + ln(nd.noDocs/ndw.noDocWords)) * (" + k1 + " + 1) * (f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/adl.avgDL)))) Okapi from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) nd cross join (select avg(docLen) avgDL from q_docLen) adl where ad.gender = '" + gender + "' and word in " + words(noWords) + " group by f.id_document order by Okapi desc, f.id_document limit " + top + ";"
          }
          else if (query == 2) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select f.id_document, sum((1+ln(nd.noDocs/ndw.noDocWords)) * (" + k1 + " + 1) * (f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/adl.avgDL)))) Okapi from  document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) nd cross join (select avg(docLen) avgDL from q_docLen) adl where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and word in " + words(noWords) + " group by f.id_document order by Okapi desc, f.id_document limit " + top + ";"
          }
          else if (query == 3) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word ) select f.id_document, sum((1+ln(nd.noDocs/ndw.noDocWords)) * (" + k1 + " + 1) * (f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/adl.avgDL)))) Okapi from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) nd cross join (select avg(docLen) avgDL from q_docLen) adl where ad.gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and word in " + words(noWords) + " group by f.id_document order by Okapi desc, f.id_document limit " + top + ";"
          }
          else if (query == 4) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select f.id_document, sum((1+ln(nd.noDocs/ndw.noDocWords)) * (" + k1 + " + 1) * (f.tf/(f.tf + " + k1 + " * (1 - " + b + " + " + b + " * dl.docLen/adl.avgDL)))) Okapi from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location inner join q_docLen dl on dl.id_document = f.id_document inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) nd cross join (select avg(docLen) avgDL from q_docLen) adl where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and word in " + words(noWords) + " group by f.id_document order by Okapi desc, f.id_document limit " + top + ";"
          }
          System.out.println(q)
          val printFile = "TopK_Documents_Okapi_q" + query + "_w" + (noWords + 1) + "_" + gender + ".sql"
          val pw = new PrintWriter(new File(printFile))
          pw.println(q)
          pw.close()
        }
      }
    }

    System.out.println("TopK Documents TFIDF")
    for (query <- 1 to 4) {
      for (gender <- genders) {
        for (noWords <- 0 to 2) {
          if (query == 1) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author where gender = '" + gender + "' group by f.id_word) select f.id_document, sum((1+ln(dl.noDocs/ndw.noDocWords)) * f.tf) TFIDF from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) dl where ad.gender = '" + gender + "' and word in " + words(noWords) + " group by f.id_document order by TFIDF desc, f.id_document limit " + top + ";"
          }
          else if (query == 2) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time where gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " group by f.id_word) select f.id_document, sum((1+ln(dl.noDocs/ndw.noDocWords)) * f.tf) TFIDF from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) dl where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and word in " + words(noWords) + " group by f.id_document order by TFIDF desc, f.id_document limit " + top + ";"
          }
          else if (query == 3) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document ), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location where gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word) select f.id_document, sum((1+ln(dl.noDocs/ndw.noDocWords)) * f.tf) TFIDF from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join location_dimension ld on ld.id_location = f.id_location inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) dl where ad.gender = '" + gender + "' and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and word in " + words(noWords) + " group by f.id_document order by TFIDF desc, f.id_document limit " + top + ";"
          }
          else if (query == 4) {
            q = "with q_docLen as (select f.id_document, sum(f.count) docLen from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender='" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_document), q_noDocWords as (select f.id_word, count(distinct f.id_document) noDocWords from document_facts f inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location where gender='" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " group by f.id_word ) select f.id_document, sum((1+ln(dl.noDocs/ndw.noDocWords)) * f.tf) TFIDF from document_facts f inner join word_dimension wd on wd.id_word = f.id_word inner join author_dimension ad on ad.id_author = f.id_author inner join time_dimension td on td.id_time = f.id_time inner join location_dimension ld on ld.id_location = f.id_location inner join q_noDocWords ndw on ndw.id_word = f.id_word cross join (select count(id_document) noDocs from q_docLen) dl where ad.gender = '" + gender + "' and td.full_date between " + startDate + " and " + endDate + " and ld.x between " + startX + " and " + endX + " and ld.y between " + startY + " and " + endY + " and word in " + words(noWords) + " group by f.id_document order by TFIDF desc, f.id_document limit " + top + ";"
          }
          System.out.println(q)
          var printFile = "TopK_Documents_TFIDF_q" + query + "_w" + (noWords + 1) + "_" + gender + ".sql"
          var pw = new PrintWriter(new File(printFile))
          pw.println(q)
          pw.close()
        }
      }
    }

  }
}

