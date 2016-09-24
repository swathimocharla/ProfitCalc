/**
 * Created by Swathi on 24-09-2016.
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext

object profitCalc {



  def main(args: Array[String]) {
    val NumPartitions = 5
    val sc = new SparkContext("local", "data team")

    val calcList1 = sc.textFile("ProductSalesData.txt").map {
      line =>
        val Array(productID, transactionType, quantity, price, transactionDate) = line.split(",")
        (productID.toInt, transactionType, quantity.toInt, price.toDouble, transactionDate)
    }

    //converting Date to an Int type for easier comparision
    def convDatetoInt(sdate: String): Int = {

      val sdf = new SimpleDateFormat("MMM-dd")
      val date1: Date = sdf.parse(sdate)
      val sdf2 = new SimpleDateFormat("MMdd")
      val d1: String = sdf2.format(date1)
      val intDate: Int = Integer.valueOf(d1)
      //println(intDate)
      return intDate
    }

    print("here is the updated rdd with date converted to Int and the data sorted by the product ID")
    val t = calcList1.map(x => (x._1, x._2, x._3, x._4, convDatetoInt(x._5))).sortBy(h => h._1)
    t.foreach(println)

    val i = calcList1.groupBy(_._1).distinct().sortBy(_._1).keys
    i.foreach(println)

    val iter = i.toLocalIterator
    var profits: Map[Int, Double] = Map()

    //for(h <- 1 to 3){
    while (iter.hasNext) {
      val x = t.toLocalIterator
      val y = t.toLocalIterator

      var p = iter.next()
      var profit = 0.0
      var k = x.next()
      var j = y.next()
      var now = false
      while (x.hasNext && now == false) {

        if (k._1 == p)
          now = true

        else {
          k = x.next()
          j = y.next()
        }
      }
      //j=k

      if (j._2 == "S")
        println("Exception here")

      var b, s = 0

      b = j._3
      //s= k._3
      while ((x.hasNext && k._1 == p)) {

        if ((k._1 == p) && (j._1 == p)) {
          if (k._2 == "B") {
            k = x.next()
            s = k._3
            //println("k here "+k)

          }

          if (k._2 == "S") {
            if (b >= s) {
              println(profit + " (" + s + " *" + k._4 + ") - (" + s + "*" + j._4 + ")")
              profit = profit + ((s * k._4) - (s * j._4))
              println("=========================")
              b = b - s
              if (x.hasNext) {
                k = x.next()
                s = k._3
              }
            }
            else if (s > b) {
              while (k._5 > j._5 && s != 0) {
                if (b > s && s != 0) {
                  println(profit + " (" + s + " *" + k._4 + ") - (" + s + "*" + j._4 + ")")
                  profit = profit + ((s * k._4) - (s * j._4))
                  println("=========================")
                  b = b - s
                  s = 0


                }
                else {
                  println(profit + " (" + b + " *" + k._4 + ") - (" + b + "*" + j._4 + ")")
                  profit = profit + ((b * k._4) - (b * j._4))
                  println("=========================")
                  s = (s - b)

                  var done = false
                  while (y.hasNext && !done && (k._5 > j._5)) {
                    j = y.next()
                    if (j._2 == "B") {
                      b = j._3
                      done = true;
                      //break
                    }
                  }


                }
              }

            }

          }

        }
        println("profit in the end:" + profit)
        profits += p -> profit

      }


    }

    println("***************")
    println("Profits for each Product ID")
    profits.foreach(println)
    println("***************")
  }
}
