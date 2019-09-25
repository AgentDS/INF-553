object Test_List {
	def main(args: Array[String]) {
		val site = "Runoob" :: ("Google" :: ("Baidu" :: Nil))
		val num = Nil

		println("First site: " + site.head)
		println("Last site: " + site.tail)
		println("Check whether site is empty: " + site.isEmpty)
		println("Check whether num is empty: " + num.isEmpty)
	}
}