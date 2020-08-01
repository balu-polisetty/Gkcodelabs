
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.types.StringType
import com.typesafe.config._
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.functions.col
import java.text.SimpleDateFormat
import java.util.{Calendar,Date}


object DailydataInjestAndRefine extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  // Initalizing spark session
  val spark = SparkSession.builder()
    .appName("Injesting Daily Data")
    .master("local[*]")
    .getOrCreate()

  // Giving schema by hardcoding
  /*  val landingschema = StructType(
  StructField("Sale_Id", StringType, true) ::
  StructField("Product_Id",StringType,true) ::
  StructField("Quantity_Sold",IntegerType,true) ::
  StructField("Vendor_Id",StringType,true) ::
  StructField("Sale_date",TimestampType,true) ::
  StructField("Sale_Amount",DoubleType,true) ::
  StructField("Sale_Curancy",StringType,true) :: Nil
)*/

  // Reading data by Hard-coading
  //.csv("file:///C://Users/balu/desktop/Inputs/Inputs/Sales_Landing/SalesDump_04062020/SalesDump.dat")

// Reading the file from config file
  val config = ConfigFactory.load("application.conf")
  val inputlocation = config.getString("inputpath")
  val outputlocation = config.getString("outputpath")
//  println(inputlocation)

  val td = new Date
  val sdf = new SimpleDateFormat("ddMMyyyy")
  val today = sdf.format(td)
//  println(today)

  val calander = Calendar.getInstance()
  calander.roll(Calendar.DAY_OF_MONTH,-1)
  val yesterday = sdf.format(calander.getTime)
//  println(yesterday)

  //Reading the schema string from config file
  val raw_schema = config.getString("scheema")
//  println(raw_schema)

  //Converting the string into schema object
  val schema = StructType(raw_schema.split(",").map(fieldname => StructField(fieldname,StringType,true)))
//  println(schema)

  //Reading today's data into DataFrame
   val landingFileDf = spark.read.schema(schema).option("delimiter","|").option("inferschema","true")
     .csv(inputlocation + "SalesDump_" + yesterday)
//   landingFileDf.show()
//   landingFileDf.printSchema()

   //Changin the data types
  val df = landingFileDf.withColumn("Quantity_Sold",col("Quantity_Sold").cast("int"))
      .withColumn("Sale_date",col("Sale_date").cast("timestamp"))
      .withColumn("Sale_Amount",col("Sale_Amount").cast("double"))
//  df.show()
//  df.printSchema()

  // Filtering the data
  val invalidDf = df.filter(df.col("Quantity_Sold").isNull || df.col("Vendor_Id").isNull)
  val validDf = df.filter(df.col("Quantity_Sold").isNotNull && df.col("Vendor_Id").isNotNull)

  validDf.write.mode("overwrite").option("delimiter","|").option("header","true")
    .csv(outputlocation + "validData_" + yesterday)

  invalidDf.write.mode("overwrite").option("header","true").option("delimiter","|")
    .csv(outputlocation + "holdData_" + yesterday)
}