import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object hw1Task1 {
	def main(args: Array[String]) {
		val path = "/Users/liangsiqi/Documents/Dataset/yelp_dataset/"
        val userFile = "user.json"
        val reviewFile = "review.json"
        val businessFile = "business.json"

        val sc = new SparkContext()
		val userRDD = sc.textFile(path + userFile)
		val userRDDjson = userRDD.map(json.loads)

	}
}