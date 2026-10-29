package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.domain.{AnalyticsApplicationStatus, AnalyticsEventType}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  array_compact,
  array_distinct,
  array_except,
  array_union,
  col,
  collect_set,
  explode,
  flatten,
  greatest,
  least,
  lit,
  max,
  min,
  size,
  when
}

import java.time.Duration

/** The pseudonymized per-application earliest time each status was entered: `silver/application_stage_times`.
  *
  * The grain is one row per `applicationId`. Merging is idempotent and order-independent: each stage keeps its earliest
  * time, the last event its latest, the subject tokens their union, and `terminalAt` and `expiresAt` are recomputed
  * from the merged row. The `CANDIDATE_HIRED` event repeats the Hired status transition and is excluded, as in the
  * funnel.
  */
private[analytics] object ApplicationStageTimes {
  private val ApplicationCreated = AnalyticsEventType.ApplicationCreated.wire
  private val ApplicationStatusChanged = AnalyticsEventType.ApplicationStatusChanged.wire

  /** The stage columns in lifecycle order, each the earliest time the application entered that status. */
  val StageColumns: Vector[String] = Vector(
    Columns.CreatedAt,
    Columns.AcceptedAt,
    Columns.DeclinedAt,
    Columns.InterviewAt,
    Columns.HiredAt,
    Columns.RejectedAt
  )

  private val MergeCondition = "target.applicationId = source.applicationId"

  /** `terminalAt = least(declinedAt, hiredAt, rejectedAt)`; null while the application is open. */
  private def terminalAt(declined: Column, hired: Column, rejected: Column): Column = least(declined, hired, rejected)

  /** D1: a terminal application expires after the terminal retention, an open one after the open retention. */
  private def expiresAt(terminal: Column, lastEvent: Column, retention: AnalyticsRetentionSettings): Column =
    when(terminal.isNotNull, terminal + lit(Duration.ofDays(retention.stageTimesTerminalDays.toLong)))
      .otherwise(lastEvent + lit(Duration.ofDays(retention.stageTimesOpenDays.toLong)))

  /** One row per application from Silver-shaped lifecycle facts (the same frame Silver is merged from). */
  def source(facts: DataFrame, retention: AnalyticsRetentionSettings): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms.validateSilverSchema(facts).map { validated =>
      def enteredStatus(status: AnalyticsApplicationStatus): Column =
        col(Columns.EventType) === lit(ApplicationStatusChanged) && col(Columns.NewStatus) === lit(status.wire)
      def earliest(entered: Column): Column = min(when(entered, col(Columns.OccurredAt)))
      val lifecycle = validated.filter(
        col(Columns.ApplicationId).isNotNull &&
          col(Columns.EventType).isin(ApplicationCreated, ApplicationStatusChanged)
      )
      val perApplication = lifecycle
        .groupBy(Columns.ApplicationId)
        .agg(
          // Every lifecycle event carries the candidate, so the smallest token is the one candidate token.
          min(col(Columns.SubjectToken)).as(Columns.SubjectToken),
          array_compact(array_distinct(flatten(collect_set(col(Columns.SubjectTokens))))).as(Columns.SubjectTokens),
          earliest(col(Columns.EventType) === lit(ApplicationCreated)).as(Columns.CreatedAt),
          earliest(enteredStatus(AnalyticsApplicationStatus.Accepted)).as(Columns.AcceptedAt),
          earliest(enteredStatus(AnalyticsApplicationStatus.Declined)).as(Columns.DeclinedAt),
          earliest(enteredStatus(AnalyticsApplicationStatus.Interview)).as(Columns.InterviewAt),
          earliest(enteredStatus(AnalyticsApplicationStatus.Hired)).as(Columns.HiredAt),
          earliest(enteredStatus(AnalyticsApplicationStatus.Rejected)).as(Columns.RejectedAt),
          max(col(Columns.OccurredAt)).as(Columns.LastEventAt)
        )
        .withColumn(
          Columns.TerminalAt,
          terminalAt(col(Columns.DeclinedAt), col(Columns.HiredAt), col(Columns.RejectedAt))
        )
        .withColumn(Columns.ExpiresAt, expiresAt(col(Columns.TerminalAt), col(Columns.LastEventAt), retention))
      perApplication.select(AnalyticsTableSchemas.stageTimes.map { case (name, _) => col(name) }*)
    }

  /** Merges the pre-aggregated source into the table. A matched row is rewritten only when a stage, the last event or
    * the token set would change. `subjectToken` is never updated, which pins the first candidate token.
    */
  def merge(
      spark: SparkSession,
      path: String,
      source: DataFrame,
      retention: AnalyticsRetentionSettings
  ): Unit = {
    def target(name: String): Column = col(s"target.$name")
    def incoming(name: String): Column = col(s"source.$name")
    val stages = StageColumns.map(name => name -> least(target(name), incoming(name)))
    val lastEvent = greatest(target(Columns.LastEventAt), incoming(Columns.LastEventAt))
    val mergedStage = stages.toMap
    val terminal = terminalAt(
      mergedStage(Columns.DeclinedAt),
      mergedStage(Columns.HiredAt),
      mergedStage(Columns.RejectedAt)
    )
    val storedTokens = target(Columns.SubjectTokens)
    val incomingTokens = incoming(Columns.SubjectTokens)
    // array_compact types the result as non-null elements, which is the declared column type.
    val tokens = array_compact(array_union(storedTokens, incomingTokens))
    val changed = (stages :+ (Columns.LastEventAt -> lastEvent))
      .map { case (name, merged) => !target(name).eqNullSafe(merged) }
      .reduce(_ || _) ||
      size(array_except(incomingTokens, storedTokens)) > lit(0)
    val updates: Map[String, Column] = mergedStage ++ Map(
      Columns.LastEventAt -> lastEvent,
      Columns.SubjectTokens -> tokens,
      Columns.TerminalAt -> terminal,
      Columns.ExpiresAt -> expiresAt(terminal, lastEvent, retention)
    )
    val _ = DeltaTables
      .forPath(spark, path)
      .as("target")
      .merge(source.as("source"), MergeCondition)
      .whenMatched(changed)
      .update(updates)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  /** Creates or validates the table, then merges the lifecycle facts into it. */
  def mergeFacts(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      facts: DataFrame,
      retention: AnalyticsRetentionSettings
  ): Either[AnalyticsError, Unit] = {
    AnalyticsTableSchemas.createOrValidate(
      spark,
      paths.applicationStageTimes,
      AnalyticsDeltaCatalog.ApplicationStageTimes
    )
    source(facts, retention).flatMap { merged =>
      if (!AnalyticsTableSchemas.matches(merged.schema, AnalyticsTableSchemas.stageTimes))
        Left(AnalyticsError.DeltaSchemaMismatch(paths.applicationStageTimes))
      else Right(merge(spark, paths.applicationStageTimes, merged, retention))
    }
  }

  /** Applications with any event naming an active deletion marker. The whole application is left out, not just that
    * event, so an erased recruiter cannot leave a partial row behind.
    */
  private def withoutMarkedApplications(facts: DataFrame, markers: DataFrame): DataFrame = {
    val markedApplications = facts
      .filter(col(Columns.ApplicationId).isNotNull)
      .select(col(Columns.ApplicationId), explode(col(Columns.SubjectTokens)).as("_markedToken"))
      .join(
        markers.select(col(Columns.SubjectToken).as("_activeMarker")).distinct(),
        col("_markedToken") === col("_activeMarker"),
        "left_semi"
      )
      .select(Columns.ApplicationId)
      .distinct()
    // A USING join moves the join column first; the Silver shape is positional, so restore the original order.
    facts.join(markedApplications, Seq(Columns.ApplicationId), "left_anti").select(facts.columns.map(col)*)
  }

  /** Builds the first stage times from Silver, with the same merge, leaving out applications that involve an active
    * deletion marker. Callers invoke it only when the table did not exist: an existing table, even an empty one, is
    * never backfilled, because rebuilding it after an erasure purge from the Silver events that survive would recreate
    * partial rows. Silver holds at most the Silver retention, so this recovers only what Silver still knows.
    */
  def backfillFromSilver(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      retention: AnalyticsRetentionSettings,
      markers: DataFrame
  ): Unit =
    if (DeltaTables.exists(spark, paths.silver)) {
      val unmarked = withoutMarkedApplications(DeltaTables.read(spark, paths.silver), markers)
      val _ = mergeFacts(spark, paths, unmarked, retention).fold(throw _, identity)
    }
}
