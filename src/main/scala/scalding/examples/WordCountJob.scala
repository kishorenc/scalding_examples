package scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) {
      line : String => tokenize(line)
    }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )

  // split a piece of text into individual words
  def tokenize(text : String) : Array[String] = {
    text.toLowerCase.split(" ")
  }
}

object WordCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scalding.examples.WordCountJob",
    "--input", "/tmp/files",
    "--output", "/tmp/wordcount.txt",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}