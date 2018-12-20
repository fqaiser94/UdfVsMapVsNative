spark-shell --packages="org.vegas-viz:vegas_2.11:0.3.11,org.vegas-viz:vegas-spark_2.11:0.3.11"

// basic setup

val testDf = spark.range(10000000).toDF.cache

testDf.count

val path = "temp.parquet/"
testDf.write.mode("overwrite").parquet(path)
val testDfParquet = spark.read.parquet(path)

def averageTime[R](block: => R, numIter: Int = 10): Unit = {
	val t0 = System.nanoTime()
	(1 to numIter).foreach( _ => block )
	val t1 = System.nanoTime()
	val averageTimeTaken = (t1 - t0) / numIter
	val timeTakenMs = averageTimeTaken / 1000000
	println("Elapsed time: " + timeTakenMs + "ms")
}

// spark native functions

val resultSparkNative = testDf.withColumn("addOne", 'id + 1)

resultSparkNative.show(5)

val resultSparkNative = testDf.withColumn("addOne", 'id.plus(1))

resultSparkNative.show(5)

resultSparkNative.explain 

averageTime { resultSparkNative.count }

val resultSparkNativeFilterParquet = testDfParquet.filter('id.plus(1) === 2)

resultSparkNativeFilterParquet.explain

averageTime { resultSparkNativeFilterParquet.count }

// user defined functions

import org.apache.spark.sql.functions.udf

val addOneUdf = udf { x: Long => x + 1 }

val resultUdf = testDf.withColumn("addOne", addOneUdf('id))

resultUdf.show(5)

resultUdf.explain

averageTime { resultUdf.count }

val resultUdfFilterParquet = testDfParquet.filter(addOneUdf('id) === 2)

resultUdfFilterParquet.explain

averageTime { resultUdfFilterParquet.count }

// map functions

case class foo(id: Long, addOne: Long)

val resultMap = testDf.map { r => foo(r.getAs[Long](0), r.getAs[Long](0) + 1) }

resultMap.show(5)

resultMap.explain

averageTime { resultMap.count }

val resultMapFilterParquet = testDfParquet.map{ r => foo(r.getAs[Long](0), r.getAs[Long](0) + 1) }.filter('addOne === 2)

resultMapFilterParquet.explain

averageTime { resultMapFilterParquet.count }

// custom spark native function

wget https://github.com/apache/spark/archive/v2.4.0-rc5.tar.gz
tar -zxvf v2.4.0-rc5.tar.gz

cat <<EOF >> ~/spark-2.4.0-rc5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/CustomExpressions.scala
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, LongType}

case class Add_One_Custom_Native(child: Expression) extends UnaryExpression {

  override def dataType: DataType = LongType

  override def prettyName: String = "addOneSparkNative"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = defineCodeGen(ctx, ev, c => s"$c + 1")
}
EOF

cat <<EOF >> ~/spark-2.4.0-rc5/sql/core/src/main/scala/org/apache/spark/sql/CustomFunctions.scala
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._

object CustomFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)

  def addOneCustomNative(x: Column): Column = withExpr {
    Add_One_Custom_Native(x.expr)
  }
}
EOF

cat <<EOF >> ~/spark-2.4.0-rc5/sql/core/src/test/scala/org/apache/spark/sql/CustomFunctionsSuite.scala
package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class CustomFunctionsSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("addOneCustomNative") {
    import org.apache.spark.sql.CustomFunctions._

    val input = Seq[java.lang.Integer](1, 21, null).toDF("id")

    val result = input.withColumn("addOne", addOneCustomNative('id))
    val expected = input.withColumn("addOne", 'id + 1)

    checkAnswer(result, expected)
  }
}
EOF

cd ~/spark-2.4.0-rc5
./build/sbt
testOnly *.CustomFunctionsSuite

./build/sbt "testOnly *.CustomFunctionsSuite"

mvn test -Dtest=CustomFunctionsSuite

./dev/make-distribution.sh --name custom-spark --tgz
tar -zxvf spark-2.4.0-bin-custom-spark.tgz

export SPARK_HOME=~/spark-2.4.0-rc5/spark-2.4.0-bin-custom-spark
export PATH=$SPARK_HOME/bin:$PATH
spark-shell --packages="org.vegas-viz:vegas_2.11:0.3.11,org.vegas-viz:vegas-spark_2.11:0.3.11"

import org.apache.spark.sql.CustomFunctions.addOneCustomNative

val resultCustomNative = testDf.withColumn("addOne", addOneCustomNative('id))

resultCustomNative.show(5)

resultCustomNative.explain

val resultCustomNativeFilterParquet = testDfParquet.filter(addOneCustomNative('id) === 2)

resultCustomNativeFilterParquet.explain

averageTime { resultCustomNativeFilterParquet.count }

// graphs setup

import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._
import org.apache.spark.sql.Dataset

def time[R](block: => R) = {
	val t0 = System.nanoTime()
	block
	val t1 = System.nanoTime()
	val timeTakenMs = (t1 - t0) / 1000000
	timeTakenMs
}

case class ResultsSchema(method: String, iteration: Int, timeTaken: Long)

def genResults(methods: Map[String, org.apache.spark.sql.Dataset[_ >: foo with org.apache.spark.sql.Row <: Serializable]]) = {
	methods.keys.map { method => { 
	(1 to 1000).map { iter => 
		val timeTaken = time { methods.get(method).get.count } 
		ResultsSchema(method, iter, timeTaken)
	}}}.flatten.toSeq.toDS
}

def genSummary(ds: Dataset[ResultsSchema]) = {
	ds
	.groupBy('method)
	.agg(
		min('timeTaken) as "min",
		round(mean('timeTaken), 1) as "mean",
		max('timeTaken) as "max", 
		round(stddev('timeTaken), 1) as "std", 
		count("*") as "sampleSize")
	.orderBy('method)
}


def genGraph(ds: Dataset[ResultsSchema]) = {
	Vegas("bar chart", width=1000, height=200).
	withDataFrame(ds.withColumn("count", lit(1))).
	filter("datum.timeTaken < 500").
	mark(Bar).
	encodeX("timeTaken", Quantitative, scale=Scale(bandSize=50), axis=Axis(title="Time Taken (ms)")).
	encodeY("count", Quantitative, aggregate=AggOps.Count, axis=Axis(title="Count")).
	encodeColumn("method", title="Method").
	configCell(width = 200, height = 200)
}

// cache count

val methodsCacheCount = Map(
	"Native" -> resultSparkNative, 
	"UDF" -> resultUdf, 
	"Map" -> resultMap, 
	"Custom Native" -> resultCustomNative
)

val resultsCacheCount = genResults(methodsCacheCount)
genSummary(resultsCacheCount).show
genGraph(resultsCacheCount).show

val path = "~/temp/resultsCacheCount.parquet"
resultsCacheCount.write.mode("overwrite").parquet(path)
val resultsCacheCount = spark.read.parquet(path).as[ResultsSchema].cache
resultsCacheCount.count

// parquet filter

val methodsParquetFilter = Map(
	"Native" -> resultSparkNativeFilterParquet, 
	"UDF" -> resultUdfFilterParquet, 
	"Map" -> resultMapFilterParquet, 
	"Custom Native" -> resultCustomNativeFilterParquet
)

val resultsParquetFilter = genResults(methodsParquetFilter)
genSummary(resultsParquetFilter).show
genGraph(resultsParquetFilter).show

val path = "~/temp/resultsParquetFilter.parquet"
resultsParquetFilter.write.mode("overwrite").parquet(path)
val resultsParquetFilter = spark.read.parquet(path).as[ResultsSchema]
