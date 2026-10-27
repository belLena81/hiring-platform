package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.*

import scala.jdk.CollectionConverters.*

/** Shapes derived from [[AnalyticsDeltaCatalog]], and strict create-or-validate for the tables it declares. */
private[analytics] object AnalyticsTableSchemas {
  type Shape = Vector[(String, DataType)]

  private def shapeOf(columns: Vector[DeltaColumn]): Shape = columns.map(column => column.name -> column.dataType)

  /** Normalized event fields before the retention columns are added. */
  val silver: Shape = shapeOf(AnalyticsDeltaCatalog.SilverEventColumns)
  val expiry: Shape = shapeOf(AnalyticsDeltaCatalog.ExpiryColumns)
  val lateFacts: Shape = AnalyticsDeltaCatalog.LateFacts.shape
  val funnel: Shape = AnalyticsDeltaCatalog.FunnelGold.shape
  val timeToHire: Shape = AnalyticsDeltaCatalog.TimeToHireGold.shape
  val skills: Shape = AnalyticsDeltaCatalog.SkillsGold.shape
  val manifests: Shape = AnalyticsDeltaCatalog.RunManifests.shape
  val quarantine: Shape = AnalyticsDeltaCatalog.Quarantine.shape
  val bronze: Shape = AnalyticsDeltaCatalog.Bronze.shape

  /** The table a merge target must be, resolved from its location. Unknown locations are Silver, as before. */
  def targetOf(paths: AnalyticsLakehousePaths, path: String): DeltaTableSpec =
    AnalyticsDeltaCatalog.specAt(paths, path).getOrElse(AnalyticsDeltaCatalog.Silver)

  /** Nullable-only schema for source frames, whose Spark-derived nullability is not authoritative. */
  def struct(shape: Shape): StructType = StructType(shape.map { case (name, dataType) =>
    StructField(name, dataType, nullable = true)
  })

  /** Timestamp literal `days` after `now`, used for retention expiry columns. */
  def expiryAt(now: java.time.Instant, days: Long): Column =
    lit(java.sql.Timestamp.from(now.plus(java.time.Duration.ofDays(days))))

  /** Single non-null `subjectToken` column holding the given deletion-marker tokens. */
  def markerFrame(spark: SparkSession, tokens: Vector[SubjectToken]): DataFrame =
    spark.createDataFrame(
      tokens.map(token => Row(token.value)).asJava,
      StructType(Seq(StructField(Columns.SubjectToken, StringType, nullable = false)))
    )

  /** Source frames are compared by names and types only; Delta enforces NOT NULL and CHECK when it writes. */
  def matches(actual: StructType, shape: Shape): Boolean =
    actual.fields.toVector.map(field => field.name -> field.dataType.simpleString) ==
      shape.map { case (name, dataType) => name -> dataType.simpleString }

  /** Creates the declared table when absent, then requires the stored table to equal the declaration exactly: column
    * names, types, nullability and the set of CHECK constraints. Any difference fails closed.
    */
  def createOrValidate(spark: SparkSession, path: String, spec: DeltaTableSpec): Unit = {
    if (!DeltaTables.exists(spark, path)) create(spark, path, spec)
    if (!storedMatches(spark, path, spec)) throw AnalyticsError.DeltaSchemaMismatch(path)
  }

  private def create(spark: SparkSession, path: String, spec: DeltaTableSpec): Unit = {
    val location = SparkPhysicalLocation.resolve(path)
    val builder = DeltaTable.createIfNotExists(spark).location(location).addColumns(spec.schema)
    val _ = spec.properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
    // Constraints are added to the empty new table, so no existing row needs validation. A concurrent creator may
    // have added the same constraint first, which the strict validation that follows accepts.
    spec.checks.foreach { check =>
      if (!storedConstraints(spark, path).contains(check.name))
        try {
          val _ = spark.sql(
            s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} ADD CONSTRAINT ${check.name} CHECK (${check.expression})"
          )
        } catch {
          case error: Exception if !storedConstraints(spark, path).contains(check.name) => throw error
        }
    }
  }

  /** Delta stores a constraint as the parser's tokens joined by spaces, so compare without whitespace. */
  private def normalize(expression: String): String = expression.replaceAll("\\s+", "")

  private def storedConstraints(spark: SparkSession, path: String): Map[String, String] =
    DeltaLogFactory
      .system(spark, path)
      .update()
      .metadata
      .configuration
      .collect {
        case (key, expression) if key.startsWith(AnalyticsDeltaCatalog.ConstraintPropertyPrefix) =>
          key.stripPrefix(AnalyticsDeltaCatalog.ConstraintPropertyPrefix) -> normalize(expression)
      }

  private def storedMatches(spark: SparkSession, path: String, spec: DeltaTableSpec): Boolean = {
    val metadata = DeltaLogFactory.system(spark, path).update().metadata
    val columns =
      metadata.schema.fields.toVector.map(field => (field.name, field.dataType.simpleString, field.nullable))
    val expectedColumns = spec.columns.map(column => (column.name, column.dataType.simpleString, column.nullable))
    val constraints = storedConstraints(spark, path)
    columns == expectedColumns &&
    constraints == spec.checks.map(check => check.name -> normalize(check.expression)).toMap
  }
}
