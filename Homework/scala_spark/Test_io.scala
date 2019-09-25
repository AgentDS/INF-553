import java.io._
import scala.io.Source

object Test_io {
	def main(args: Array[String]) {
		// write file
		val writer = new PrintWriter(new File("test_io.txt"))

		writer.write("菜鸟教程")
		writer.close()

		// read file
		println("File content: ")
		Source.fromFile("test_io.txt").foreach{
			print
		}
	}
}