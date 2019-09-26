import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json._

case class UserInfo(
		user_id: String,
	    name: String,
	    review_count: Int,
	    yelping_since: String,  // like '2013-02-21 22:29:06'
	    useful: Int,
	    funny: Int,
	    cool: Int,
	    elite: String,  // like '2015,2016,2017'
	    friends: String,  // like 'xx, xxx, xxx'
	    fans: Int,
	    average_stars: Float,
	    compliment_hot: Int,
	    compliment_more: Int,
	    compliment_profile: Int,
	    compliment_cute: Int,
	    compliment_list: Int,
	    compliment_note: Int,
	    compliment_plain: Int,
	    compliment_cool: Int,
	    compliment_funny: Int,
	    compliment_writer: Int,
	    compliment_photos: Int
)


object siqi_liang_task1 {

	// implicit val formats = DefaultFormats

    // def json_parse(jsonObj: String) : UserInfo = {
    // 	val jValue = parse(jsonObj)
    // 	val user = jValue.extract[UserInfo]
    // 	return user
    // }


	def main(args: Array[String]) {
		// val userFile = args(0)
		// val outFile = args(1)
		
		val path = "/Users/liangsiqi/Documents/Dataset/yelp_dataset/"
        val userFile = "user.json"
        val reviewFile = "review.json"
        val businessFile = "business.json"

        val sc = new SparkContext()
		val userRDD = sc.textFile(path + userFile)
		val userRDDjson = userRDD.map(x => Json.parse(x))
		println(userRDD.take(1).JsValue)

	}
}