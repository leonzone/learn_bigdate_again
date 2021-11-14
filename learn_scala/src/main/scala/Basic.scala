import java.io.{FileNotFoundException, FileReader, IOException}
import scala.util.matching.Regex

object Basic {

  def main(args: Array[String]): Unit = {
    testVal()
    testFor()
    testCollection()
    testClass()

    def sayHello(name: String): String = {
      //返回不需要return,最后一行就是返回值
      "Hello," + name
    }
    //Scala中的函数是一等公民，可以直接将函数作为值赋值给变量
    //将函数赋值给变量时，必须在函数后面加上“ _”或“(_)”
    val hi = sayHello _
    val hey = sayHello(_)
    println(hi("Ada"))
    println(hey("Reiser"))

    val hello = (name: String) => println("hello," + name)
    hello("MilkTea")

    //    高阶函数:接收函数作为参数的函数，或返回值为函数的函数，被称作高阶函数
    def higherOrderHi(func: (String) => Unit, name: String): Unit = {
      func(name)
    }

    higherOrderHi(hello, "Higher")

    println(Array(1, 2, 3, 4, 5).map(_ * 10).mkString("Array(", ", ", ")"))
    Array(1, 2, 3, 4, 5).map(_ * 10).foreach(println)

    (1 to 50).filter(_ % 10 == 0).foreach(println)

    val res = Array(1, 2, 3, 4, 5).reduceLeft(_ * _)
    val res2 = Array(1, 2, 3, 4, 5).product
    println(res)

    // 闭包
    def mulBy(factor: Double) = (x: Double) => factor * x

    // 闭包由代码和代码用到的任何非局部变量定义而成
    val triple = mulBy(3)
    val half = mulBy(0.5)
    println(s"${triple(14)} ${half(14)}")

    testArray


    val m = 3

    m match {
      case 1 => println("one")
      case 2 => println("two")
      case _ => println("many")
    }

    println(matchTest(m))

    println(matchTest2(1))
    println(matchTest2(6))
    println(matchTest2("two"))
    println(matchTest2("7"))

    val alice = Person("Alice", 25)
    val bob = Person("Bob", 32)
    val charlie = Person("Charlie", 32)

    for (person <- List(alice, bob, charlie)) {
      person match {
        case Person("Alice", 25) => println("Hello Alice")
        case Person("Bob", 32) => println("Hello Alice")
        case Person(name, age) => println("Age: " + age + " year, name: " + name + "?")
      }
    }

    // 测试正则表达式
    // Scala 的正则表达式继承了 Java 的语法规则，Java 则大部分使用了 Perl 语言的规则
    val str = "Scala is scalable and cool"
    val pattern = new Regex("(S|s)cala")
    val pattern2 = "([Ss])cala".r
    println((pattern findAllIn str).mkString(","))
    println((pattern2 findAllIn str).mkString(","))

    // 异常处理
    try {
      val f = new FileReader("input.txt")
    } catch {
      case _: FileNotFoundException => print("Missing file exception")
      case _: IOException => print("IO Exception")
      case _ => print("Unkown Exception")
    } finally {
      println("Exiting finally...")
    }

    val keywords = Map("scala" -> "option", "java" -> "optional")
    println(keywords.get("java"))
    println(keywords.get("c"))

    println(display(keywords.get("java")))
    println(display(keywords.get("c")))
  }

  def display(game: Option[String]) = game match {
    case Some(s) => s
    case None => "unkown"
  }

  // case class 样例类是种特殊的类，经过优化以用于模式匹配
  case class Person(name: String, age: Int)

  // 模式匹配类似于 switch
  def matchTest(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }

  // 但是模式匹配 可以处理不同类型
  def matchTest2(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case _: Int => "scala.Int"
    case _ => "many"
  }


  /**
   * 数组
   */
  private def testArray = {

    val myList1 = Array(1, 2, 3)
    println(myList1.mkString("Array(", ", ", ")"))
    val myList2 = Array.range(1, 3)
    println(myList2.mkString("Array(", ", ", ")"))
    val myList3 = Array.range(1, 10, 2)
    println(myList3.mkString("Array(", ", ", ")"))
  }



  /**
   *
   */
  def testClass(): Unit = {
    case class Person(age: Int, name: String)

  }

  // 相当于 Java 中的接口，但可以定义抽象方法，也可以定义字段和方法的实现
  trait HasLegs {
    //抽象字段
    val legs: Int

    // 定义一个具体的方法
    def walk() {
      println("Use" + legs + "legs to walk")
    }

    // 定义一个抽象方法
    def fly()
  }


  def testCollection(): Unit = {
    println("测试集合")
    // 定义列表有两种写法 :: 和 List
    //定义 列表
    val empty = Nil
    // ::构造列表
    val nums = 1 :: (2 :: (3 :: (4 :: Nil)))
    //定义 List
    val x = List(1, 2, 3, 4)

    println(List.concat(nums, x))
    println(nums ::: x)

    //定义 Set
    val y = Set(2, 3, 3, 4)
    for (elem <- y) {
      println(elem)
    }
    //定义 元组
    val z = (10, "Runoob")
    println(z._1)

    val z1 = (10, "Runoob", 3.14, true)
    println(z1._3)

    //定义 Map
    val map = Map("one" -> 1, "two" -> 2, "three" -> 3)

    println(map("one"))
    val map2 = Map("one" -> 1, 2 -> "two", "three" -> 3)
    println(map2(2))

    // 定义 Option
    val o: Option[Int] = Some(5)

    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")

    //    def matchTest(x: Option[String]): String = x match {
    //      case Some => "one"
    //      case _ => "many"
    //    }
  }

  private def testVal() = {
    // 申明变量的关键字 var val
    var v1: String = "code"
    // 类型可以自动推断
    var v2 = 2
    //  val 相当于常量，不能不能改变其引用的对象本身
    val v3 = 3
    val v4: Double = 3.14
    val v5 = Array(1, 2, 3)
    // 但是可以改变其引用的对象的其他属性
    v5(1) = 10
  }

  def testFor(): Unit = {
    var i = 3
    while (i > 0) {
      println(i)
      i = i - 1
    }

    // for 循环不支持 continue 和 break
    // to 前闭后闭
    for (i <- 1 to 3) println(i)
    // reverse 倒序
    for (i <- (1 to 3).reverse) println(i)
    // 前闭后开
    for (i <- 1 until 3) println(i)

    //循环守卫 if 控制输出
    for (i <- 1 to 10 if i % 3 == 0) println(i)

    //循环守卫 if 控制输出
    for (i <- 1 to 3; j = 4 - i) println(j)

    // 双重 for 循环
    for (i <- 1 to 3; j <- 1 to 3) println(s"i=$i, j=$j, i+j=${i + j}")

    //    for (i <- 1 to 3; j <- 1 to 3; k <- 1 to 3) {
    //      println(s"i=$i, j=$j, k=$k,i+j=${i + j + k}")
    //    }

    val x = for (i <- 1 to 10) yield i
    println(x)
    println(x(2))
  }
}