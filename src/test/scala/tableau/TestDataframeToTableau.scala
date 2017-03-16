package tableau

import org.junit.Test
import org.junit.Assert._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.junit.BeforeClass
import org.junit.AfterClass
import org.apache.spark.sql.SQLContext
import java.io.File


object TestDataframeToTableau {
  
  var sc: SparkContext = null

  @BeforeClass
  def setup(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Dataframe to TDE")
      .setMaster("local[*]")
    sc = new SparkContext(sparkConf)
  }

  @AfterClass
  def cleanup(): Unit = {
    sc.stop()
  }
}


class TestDataframeToTableau {

  @Test
  def testSaveNumbersToExtractor():Unit = {
    val sql = new SQLContext(TestDataframeToTableau.sc)
    import sql.implicits._
    import TableauDataFrame._

    val numList = List(1,2,3,4,5)
    val df = TestDataframeToTableau.sc.parallelize(numList).toDF
    val numDf = df.select(df("_1").alias("num"))
    assertEquals(5, numDf.count)
    numDf.saveToTableau("numbers.tde")
    val f = new File("numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveLongNumbersToExtractor():Unit = {
    val sql = new SQLContext(TestDataframeToTableau.sc)
    import sql.implicits._
    import TableauDataFrame._

    val numList = List(1111111111L,-222222222222L,3333333333L)
    val df = TestDataframeToTableau.sc.parallelize(numList).toDF
    val numDf = df.select(df("_1").alias("long_num"))
    assertEquals(3, numDf.count)
    numDf.saveToTableau("long-numbers.tde")
    val f = new File("long-numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveStringsToExtractor():Unit = {
    val sql = new SQLContext(TestDataframeToTableau.sc)
    import sql.implicits._
    import TableauDataFrame._

    val charList = List("a", "b", "c", "d")
    val df = TestDataframeToTableau.sc.parallelize(charList).toDF
    val charDf = df.select(df("_1").alias("letter"))

    charDf.saveToTableau("letters.tde")
    val f = new File("letters.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveMultipleTypes():Unit = {
    val sql = new SQLContext(TestDataframeToTableau.sc)
    import TableauDataFrame._

    val jsonRDD = TestDataframeToTableau.sc.parallelize(Seq(""" 
      { "isActive": false,
        "balance": 1431.73,
        "picture": "http://placehold.it/32x32",
        "age": 35,
        "eyeColor": "blue"
      }""",
       """{
        "isActive": true,
        "balance": 2515.60,
        "picture": "http://placehold.it/32x32",
        "age": 34,
        "eyeColor": "blue"
      }""", 
      """{
        "isActive": false,
        "balance": 3765.29,
        "picture": "http://placehold.it/32x32",
        "age": 26,
        "eyeColor": "blue"
      }""")
    )
    val df = sql.read.json(jsonRDD)

    df.saveToTableau("users.tde")
    val f = new File("users.tde")
    assertTrue(f.exists())
    f.delete()
  }
}