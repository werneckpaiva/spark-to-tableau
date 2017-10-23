package tableau

import java.io.File

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit._


object TestDataframeToTableau {

  var spark: SparkSession = null

  /**
    * Create Spark context before tests
    */
  @BeforeClass
  def setUp(): Unit = {
    spark = {
      SparkSession.builder()
        .appName("DataframeToTableauTest")
        .master("local")
        .getOrCreate()
    }
  }

  /**
    * Stop Spark context after tests
    */
  @AfterClass
  def tearDown(): Unit = {
    spark.stop()
    spark = null
  }
}


class TestDataframeToTableau {

  @Test
  def testSaveNumbersToExtractor(): Unit = {
    val spark = TestDataframeToTableau.spark
    import TableauDataFrame._
    import spark.implicits._

    val df = List(1, 2, 3, 4, 5).toDF
    assertEquals(5, df.count)

    df.saveToTableau("numbers.tde")
    val f = new File("numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveLongNumbersToExtractor(): Unit = {
    val spark = TestDataframeToTableau.spark
    import TableauDataFrame._
    import spark.implicits._

    val df = List(1111111111L, -222222222222L, 3333333333L).toDF
    assertEquals(3, df.count)

    df.saveToTableau("long-numbers.tde")
    val f = new File("long-numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveDoubleToExtractor(): Unit = {
    val spark = TestDataframeToTableau.spark
    import TableauDataFrame._
    import spark.implicits._

    val df = List(2.3d, 2.4d, 3.5d).toDF
    assertEquals(3, df.count)

    df.saveToTableau("double-numbers.tde")
    val f = new File("double-numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveFloatToExtractor(): Unit = {
    val spark = TestDataframeToTableau.spark
    import TableauDataFrame._
    import spark.implicits._

    val df = List(1.1f, 2.2f, 3.3f).toDF
    assertEquals(3, df.count)

    df.saveToTableau("float-numbers.tde")
    val f = new File("float-numbers.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveStringsToExtractor(): Unit = {
    val spark = TestDataframeToTableau.spark
    import TableauDataFrame._
    import spark.implicits._

    val df = List("a", "b", "c", "d").toDF

    df.saveToTableau("letters.tde")
    val f = new File("letters.tde")
    assertTrue(f.exists())
    f.delete()
  }

  @Test
  def testSaveMultipleTypes(): Unit = {
    import TableauDataFrame._

    val jsonRDD = TestDataframeToTableau.spark.sparkContext.parallelize(Seq(
      """
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
    val df = TestDataframeToTableau.spark.read.json(jsonRDD)

    df.saveToTableau("users.tde")
    val f = new File("users.tde")
    assertTrue(f.exists())
    f.delete()
  }
}