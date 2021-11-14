//class Location(override val xc: Int, override val yc: Int, val zc: Int) extends Point(xc, yc) {
//  var z: Int = zc
//
//  def move(dx: Int, dy: Int, dz: Int) {
//    x = x + dx
//    y = y + dy
//    z = z + dz
//    println("x 的坐标点 : " + x);
//    println("y 的坐标点 : " + y);
//    println("z 的坐标点 : " + z);
//  }
//}
//
//class Point(val xc: Int, val yc: Int) {
//  var x: Int = xc
//  var y: Int = yc
//
//  def move(dx: Int, dy: Int) {
//    x = x + dx
//    y = y + dy
//    println("x 的坐标点 : " + x);
//    println("y 的坐标点 : " + y);
//  }
//}