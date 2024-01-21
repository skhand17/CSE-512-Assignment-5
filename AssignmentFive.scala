package assignment.five

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object AssignmentFive extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession:SparkSession = SparkSession.builder().
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.serializer",classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
    master("local[*]")
    .appName("lastassignment").getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)
  SedonaVizRegistrator.registerAll(sparkSession)

  val resourseFolder = System.getProperty("user.dir")+"/src/test/resources/"
  val csvPolygonInputLocation = resourseFolder + "testenvelope.csv"
  val csvPointInputLocation = resourseFolder + "testpoint.csv"
  val firstpointdata = resourseFolder + "outputdata/firstpointdata"
  val firstpolydata = resourseFolder + "outputdata/firstpolygondata"

  println("Q2.1")
  firstPointQuery()
  println("Q2.2")
  secondPointQuery()
  println("Q2.3")
  firstPloygonQuery()
  println("Q2.4")
  secondPolygonQuery()
  println("Q2.5")
  JoinQuery()

  println("Assignment Five Done!!!")

  def firstPointQuery(): Unit = {
      println("Working on reading a CSV file")
      var pointDF = sparkSession.read.format("csv").option("header","false").load(csvPointInputLocation)
      pointDF = pointDF.toDF()
      pointDF.printSchema()
      pointDF.createOrReplaceTempView("points")

     val pointDFSQL = sparkSession.sql("SELECT * FROM points where _c0 >500 AND _c1 >500")
     val updatedCount = pointDFSQL.count()
      pointDFSQL.show(updatedCount.toInt)
      println("Saving the points in the delta format")
      pointDFSQL.write.mode("overwrite").format("delta").save(firstpointdata)



  }

  def secondPointQuery(): Unit = {
    //Read the firstpointdata in delta format. Print the total count of the points.
    val firstPointDF = sparkSession.read.format("delta").load(firstpointdata)
    println("The Number of points in points data are : " +firstPointDF.count())

  }

  def firstPloygonQuery(): Unit = {
    //Read the given testenvelope.csv in csv format and write in delta format and save it named firstpolydata
    println("Working on reading a testenvelope csv file")
    var polygonDF = sparkSession.read.format("csv").option("header","false").load(csvPolygonInputLocation)
    polygonDF = polygonDF.toDF()
    polygonDF.printSchema()
    polygonDF.createOrReplaceTempView("polypoints")

    polygonDF = sparkSession.sql("SELECT _c0, _c1, _c2,_c3 FROM polypoints where _c0 >900 and _c1 >900 and _c2>900 and _c3>900")
    val updatedCount = polygonDF.count()
    polygonDF.show(updatedCount.toInt)
    polygonDF.createOrReplaceTempView("polygonDF")
    println("Saving the polypoints in delta format")
    polygonDF.write.mode("overwrite").format("delta").save(firstpolydata)

  }

  def secondPolygonQuery(): Unit = {
    //Read the firstpolydata in delta format. Print the total count of the points.
    val firstPolyPointDF =  sparkSession.read.format("delta").load(firstpolydata)
    println("Number of points in poly data are : " +firstPolyPointDF.count())
  }
  def JoinQuery(): Unit = {
    //Read the firstpointdata in delta format and find the total count for point pairs where distance between the points within a pair is less than 2.
    var firstPointDF = sparkSession.read.format("delta").load(firstpointdata)
    firstPointDF = firstPointDF.toDF()
    firstPointDF.createOrReplaceTempView("first")
    firstPointDF = sparkSession.sql("select ST_Point(cast(first._c0 as Decimal(24,20)),cast(first._c1 as Decimal(24,20))) as first from first")

    var rectPolyDf = sparkSession.read.format("delta").load(firstpolydata)
    rectPolyDf = rectPolyDf.toDF()
    rectPolyDf.createOrReplaceTempView("rectangles")
    rectPolyDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(rectangles._c0 as Decimal(24,20)),cast(rectangles._c1 as Decimal(24,20)), cast(rectangles._c2 as Decimal(24,20)), cast(rectangles._c3 as Decimal(24,20))) as rectangle from rectangles")
    rectPolyDf.createOrReplaceTempView("rectDF")
    firstPointDF.createOrReplaceTempView("firstDF")
    var finalJoinDF = sparkSession.sql("select rectDF.rectangle, firstDF.first from rectDF, firstDF where ST_Within(firstDF.first, rectDF.rectangle)")
    finalJoinDF =  finalJoinDF.toDF()
    finalJoinDF.show(100)
    finalJoinDF.createOrReplaceTempView("finalJoinDF")

    var selectedPointsDF = sparkSession.sql("Select finalJoinDF.first from finalJoinDF")
    selectedPointsDF = selectedPointsDF.toDF()
    selectedPointsDF.show(100)
    selectedPointsDF.createOrReplaceTempView("firstPoints")

    var selectedPointsDF2 =  selectedPointsDF.toDF()
    selectedPointsDF2.show(100)
    selectedPointsDF2.createOrReplaceTempView("secondPoints")

    var distancePointsDF = sparkSession.sql("Select firstPoints.first, secondPoints.first from firstPoints, secondPoints where firstPoints.first != secondPoints.first and ST_DISTANCE(firstPoints.first, secondPoints.first) < 2")
    println("######## Count Value for JoinQuery is  ################")
    println((distancePointsDF.count()/2))
  }
}