package selfPractice

import org.apache.spark._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._




object SelfPractice {
  case class schema(id:String,name:String,address1:String,address2:String,address3:String)
  println("ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678910")
  
  def main(args:Array[String]):Unit={
    
    
    val conf=new SparkConf().setMaster("local[*]").setAppName("Practice")
    
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val rdd1=spark.read.csv("file:///C:/Users/ravi1/Desktop/Demo12.csv")
    rdd1.printSchema()
    rdd1.show()
    val rdd7=spark.read.text("file:///C:/Users/ravi1/Desktop/Demo12.txt")
    rdd7.show()
    val df5=rdd7.map(x=>x.getString(0).split(","))
    df5.show()
    val df1=rdd1.rdd
    val rdd2=df1.map(x=>x.mkString(","))
    rdd2.foreach(println)
    val rdd3= rdd2.map(x=>x.split(","))
    val rdd4=rdd3.map(
        x=>schema(x(0),x(1),x(2),x(3),x(4))
                     )
   val df =rdd4.toDF()
   val df2=df.map(x=>{
     val f=Array(x.getString(2),x.getString(3),x.getString(4))
     (f,x.getString(0),x.getString(1))
   })
   val df3=df2.toDF("Address","id","name")
   df3.show()
    
    
    val st=sc.textFile("file:///C:/Users/ravi1/Desktop/Demo12.txt")
    st.foreach(println)
    val k=st.map(x=>x.split(","))
    
    val l=k.map(x=>schema(x(0),x(1),x(2),x(3),x(4)))
     val df8=l.toDF()
     val df9=df8.withColumn("address1", regexp_replace(col("address1"),"\"",""))
             .withColumn("address3", regexp_replace(col("address3"),"\"",""))
     val df10=df9.withColumn("Address", concat_ws(",",col("address1"),
                                                      col("address2"),
                                                      col("address3")              
                                                  ))
      df10.show()
                                                  val df11=df10.groupBy("id", "name").agg(collect_list("Address").as("Address"))
      df11.show()
//          val df11=df10.withColumn("Separated address fields", explode(col("Address")))
//         df11.select("id","name","Separated address fields.*").show()


val a=spark.read.format("csv").option("inferSchema",true)
            .option("header", true)
           .load("file:///C:/Users/ravi1/Desktop/Demo13.csv")
a.show()
a.createOrReplaceTempView("ABC")
    
val s="""With CTE as  (Select *, LAG(salary,1,salary) over(Partition by empid order by Year) 
  AS Old_Salary
  from  ABC) 
  Select *,salary-Old_Salary as Salary_Change from CTE"""
val b=spark.sql(s)
b.show()
    
 val win=Window.partitionBy("empid").orderBy("Year")
 val c=a.withColumn("OLD_SALARY", lag("salary",1).over(win))
 c.show()
 val d=c.withColumn("Salary Change", col("salary")-col("OLD_SALARY")).na.fill(0)
        .drop("OLD_SALARY")
 d.show()
    
    
//    val  rdd1=rdd.map(x=>x.split(","))
//    
//    val rddRow=rdd1.map(x=>Row(x(0),x(1),x(2)))
    
//  val schemaRow=new StructType()
//   .add("id", StringType)
//    .add("tdate",StringType)
//    .add("product",ArrayType(new StructType()
//    .add("address1",StringType)
//   .add("address2",StringType)
//   .add("address3",StringType)
// ))
//val df=spark.createDataFrame(df1, schemaRow)
//df.show()
//        
    
  }
  
}