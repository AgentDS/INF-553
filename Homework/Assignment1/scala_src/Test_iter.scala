object Test_iter {
	def main(args: Array[String]) {
		val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

		while (it.hasNext) {
			println(it.next())
		}



		val ita = Iterator(20,40,2,50,69, 90)
        val itb = Iterator(20,40,2,50,69, 90)
        println("ita.size 的值: " + ita.size )
        println("itb.length 的值: " + itb.length)
	}
}