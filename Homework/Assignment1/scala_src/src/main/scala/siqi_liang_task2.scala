import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object siqi_liang_task2 {
	def main(args: Array[String]) {
		val userFile = args(0)
		val outFile = args(1)
		println("User file: " + userFile)
		println("Output file: " + outFile)
		// val path = "/Users/liangsiqi/Documents/Dataset/yelp_dataset/"
  //       val userFile = "user.json"
  //       val reviewFile = "review.json"
  //       val businessFile = "business.json"

  //       val sc = new SparkContext()
		// val userRDD = sc.textFile(path + userFile)
		// val userRDDjson = userRDD.map(json.loads)

	}
}