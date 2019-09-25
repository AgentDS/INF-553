import Array._

object Test {
  def main(args: Array[String]) {
     var myMatrix = ofDim[Int](3,3)

     for (i <- 0 to 2) {
       for (j <- 0 to 2) {   
          myMatrix(i)(j) = j;
       } 
     }

     for (i <- 0 to 2) {
        for (j <- 0 to 2) {
           print(" " + myMatrix(i)(j)); 
        }
        println();
     }
     var myList1 = Array(1.9, 2.9, 3.4, 3.5)
     var myList2 = Array(8.9, 7.9, 0.4, 1.5)
     var myList3 = concat(myList1, myList2)

     for (x <- myList3) {
        println(x)
     }

     var myList4 = range(10,20,2)
     var myList5 = range(10,20)
     
     for (x <- myList4) {
        print(" " + x)
     }
     println()

     for (x <- myList5) {
        print(" " + x)
     }

  }
}
