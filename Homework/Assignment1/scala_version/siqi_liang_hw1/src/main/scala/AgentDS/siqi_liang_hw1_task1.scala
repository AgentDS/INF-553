//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import net.liftweb.json._
//
//case class UserInfo(
//                     user_id: String,
//                     name: String,
//                     review_count: Int,
//                     yelping_since: String,  // like '2013-02-21 22:29:06'
//                     useful: Int,
//                     funny: Int,
//                     cool: Int,
//                     elite: String,  // like '2015,2016,2017'
//                     friends: String,  // like 'xx, xxx, xxx'
//                     fans: Int,
//                     average_stars: Float,
//                     compliment_hot: Int,
//                     compliment_more: Int,
//                     compliment_profile: Int,
//                     compliment_cute: Int,
//                     compliment_list: Int,
//                     compliment_note: Int,
//                     compliment_plain: Int,
//                     compliment_cool: Int,
//                     compliment_funny: Int,
//                     compliment_writer: Int,
//                     compliment_photos: Int
//                   )
//
//
//object siqi_liang_task1 {
//
//  implicit val formats = DefaultFormats
//
//  def json_parse(jsonObj: String) : UserInfo = {
//    val jValue = parse(jsonObj)
//    val user = jValue.extract[UserInfo]
//    return user
//  }
//
//
//  def main(args: Array[String]) {
//    // val userFile = args(0)
//    // val outFile = args(1)
//
//    val path = "/Users/liangsiqi/Documents/Dataset/yelp_dataset/"
//    val userFile = "user.json"
//    val reviewFile = "review.json"
//    val businessFile = "business.json"
//
//    val sc = new SparkContext()
//    val userRDD = sc.textFile(path + userFile)
//    val userRDDjson = userRDD.map(x => json_parse(x))
//    println(userRDD.take(1))
//
//  }
//}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Test IO to wasb
 */
object siqi_liang_hw1_task1 {
  def main (arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WASBIOTest")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/Users/liangsiqi/Documents/Dataset/yelp_dataset/user.json")

    //find the rows which have only one digit in the 7th column in the CSV
    val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

    rdd1.saveAsTextFile("./Cout")
  }
}