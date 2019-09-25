import scala.io._

object Test_io2 {
	def main(args: Array[String]) {
		print("Enter the site name: ")
		val line = StdIn.readLine()
		println("Your enter is: " + line)
	}
}