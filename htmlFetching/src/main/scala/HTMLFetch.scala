import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.StringLike
import scala.io.Source

object HTMLFunctions {
	var urldelimit = "sparkwebpageurlout"
	var htmldelimit = "sparkwebpagehtmlout"
	def getHTML(url: String): String = {
		try{		
			return urldelimit + url + htmldelimit + scala.io.Source.fromURL(url).mkString
		}
		catch {
			case e: Exception => return urldelimit + url + htmldelimit + "sparkwebpagehtmloutempty"
		} 
	}
}

object HTMLFetch {
	def main(args: Array[String]) {
		println("staring the program");
		/*
		* This configures the spark context to run on the cluster
		*/
		val conf = new SparkConf()
			.setMaster("local[2]")
			.setAppName("Fetch HTML of URL Collection")
			.set("spark.executor.memory", "2g")
		val sc = new SparkContext(conf)
		// This is the input data read form the file given as the first argument
		val inputURLs = sc.textFile(args(0))
		val resultFile = "htmlFetchOut";

		inputURLs.map(HTMLFunctions.getHTML).saveAsTextFile(resultFile)
	}
}
