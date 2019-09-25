object Test {
   def main(args: Array[String]) {
      var a = 10;
      var b = 20;
      var c = 25;
      var d = 25;
      println("a == b = " + (a==b));
      println("a != b = " + (a!=b));
      println("a > b = " + (a>b));
      println("b >= a = " + (b>=a));
      println("b <= a = " + (b<=a));
      println("Returned Value: 5 + 7 = " + addInt(5,7));
   }
   def addInt(a:Int, b:Int) : Int = {
      var sum:Int = 0
      sum = a + b
      return sum
   }
}
