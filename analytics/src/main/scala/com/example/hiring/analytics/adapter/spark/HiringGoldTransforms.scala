package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsApplicationStatus
import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.AnalyticsHiringStageMetric
import com.example.hiring.analytics.domain.AnalyticsRetention
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  col,
  countDistinct,
  explode,
  length,
  lit,
  lower,
  percentile_approx,
  to_date,
  trim,
  unix_timestamp,
  when
}

import java.sql.Timestamp
import java.time.Instant

object HiringGoldTransforms {
  private val ApplicationCreated = AnalyticsEventType.ApplicationCreated.wire
  private val ApplicationStatusChanged = AnalyticsEventType.ApplicationStatusChanged.wire
  private val JobCreated = AnalyticsEventType.JobCreated.wire

  /** The UTC calendar day of an event time. The Spark session time zone is pinned to UTC by `SparkRuntime`. */
  private[analytics] def utcDay(occurredAt: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    to_date(occurredAt)

  private def applicationLifecycle(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms
      .validateSilverSchema(silver)
      .map(_.filter(col(Columns.EventType).isin(ApplicationCreated, ApplicationStatusChanged)))

  /** The CANDIDATE_HIRED event duplicates the Hired status transition and is deliberately excluded. */
  def funnelActivity(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    applicationLifecycle(silver).map { validated =>
      validated
        .withColumn(Columns.Day, utcDay(col(Columns.OccurredAt)))
        .groupBy(Columns.Day, Columns.EventType, Columns.NewStatus)
        .agg(
          countDistinct(col(Columns.ApplicationId)).as(Columns.ContributingApplications),
          countDistinct(col(Columns.SubjectToken)).as(Columns.ContributingSubjects)
        )
        .filter(col(Columns.ContributingSubjects) >= lit(AnalyticsRetention.MinimumContributors))
        .drop(Columns.ContributingSubjects)
    }

  /** JOB_CREATED snapshots exploded to one row per day and non-empty normalized skill. */
  private[analytics] def jobSkillDays(frame: DataFrame): DataFrame =
    frame
      .filter(col(Columns.EventType) === lit(JobCreated))
      .withColumn(Columns.Day, utcDay(col(Columns.OccurredAt)))
      .withColumn(Columns.RawSkill, explode(col(Columns.JobSkills)))
      .withColumn(Columns.Skill, lower(trim(col(Columns.RawSkill))))
      .filter(length(col(Columns.Skill)) > lit(0))

  /** Only JOB_CREATED snapshots contribute; updates and close events never alter this metric. */
  def skillPostingActivity(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms.validateSilverSchema(silver).map { validated =>
      jobSkillDays(validated)
        .dropDuplicates(Columns.EventId, Columns.AggregateId, Columns.Skill)
        .groupBy(Columns.Day, Columns.Skill)
        .agg(
          countDistinct(col(Columns.EventId)).as(Columns.Postings),
          countDistinct(col(Columns.SubjectToken)).as(Columns.ContributingSubjects)
        )
        .filter(col(Columns.ContributingSubjects) >= lit(AnalyticsRetention.MinimumContributors))
        .drop(Columns.ContributingSubjects)
    }

  /** Wide daily shape consumed by the operational AnalyticsFunnelDay projection. */
  def wideFunnelDay(silver: DataFrame): Either[AnalyticsError, DataFrame] = {
    val statusCells = AnalyticsApplicationStatus.values.toSeq.map { status =>
      status.wire.toLowerCase(java.util.Locale.ROOT) -> (col(Columns.NewStatus) === lit(status.wire))
    }
    val cells = (Columns.Created -> (col(Columns.EventType) === lit(ApplicationCreated))) +: statusCells
    val subjectCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col(Columns.SubjectToken))).as(s"${name}Subjects")
    }
    val applicationCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col(Columns.ApplicationId))).as(name)
    }
    val counts = subjectCounts ++ applicationCounts
    val subjectColumns = cells.map { case (name, _) => s"${name}Subjects" }

    applicationLifecycle(silver).map { validated =>
      validated
        .withColumn(Columns.Day, utcDay(col(Columns.OccurredAt)))
        .groupBy(Columns.Day)
        .agg(counts.head, counts.tail*)
        .filter(
          subjectColumns
            .map(name => col(name) === lit(0) || col(name) >= lit(AnalyticsRetention.MinimumContributors))
            .reduce(_ && _)
        )
        .drop(subjectColumns*)
    }
  }

  /** The stage-time columns a duration runs between: the earlier stage, then the later one. */
  private def endpoints(metric: AnalyticsHiringStageMetric): (String, String) = metric match {
    case AnalyticsHiringStageMetric.TimeToAccept        => (Columns.CreatedAt, Columns.AcceptedAt)
    case AnalyticsHiringStageMetric.TimeToInterview     => (Columns.CreatedAt, Columns.InterviewAt)
    case AnalyticsHiringStageMetric.TimeToHire          => (Columns.CreatedAt, Columns.HiredAt)
    case AnalyticsHiringStageMetric.AcceptedToInterview => (Columns.AcceptedAt, Columns.InterviewAt)
    case AnalyticsHiringStageMetric.InterviewToHire     => (Columns.InterviewAt, Columns.HiredAt)
  }

  /** Stage-time rows that have not expired at `asOf`, after checking the stored shape. */
  def currentStageTimes(stageTimes: DataFrame, asOf: Instant): Either[AnalyticsError, DataFrame] =
    Either
      .cond(
        AnalyticsTableSchemas.matches(stageTimes.schema, AnalyticsTableSchemas.stageTimes),
        stageTimes,
        AnalyticsError.InvalidStageTimesSchema
      )
      .map(_.filter(col(Columns.ExpiresAt) > lit(Timestamp.from(asOf))))

  /** Applications with both stages present and a non-negative duration, with that duration in hours. The one place the
    * eligibility rule lives: `time_to_hire` and every stage duration share it.
    */
  private def eligibleDurations(stageTimes: DataFrame, metric: AnalyticsHiringStageMetric): DataFrame = {
    val (from, to) = endpoints(metric)
    stageTimes
      .filter(col(from).isNotNull && col(to).isNotNull)
      .withColumn(Columns.Hours, (unix_timestamp(col(to)) - unix_timestamp(col(from))) / lit(3600.0))
      .filter(col(Columns.Hours) >= lit(0.0))
  }

  private def percentiles(): Seq[Column] = Seq(
    percentile_approx(col(Columns.Hours), lit(0.5), lit(10000)).as(Columns.P50Hours),
    percentile_approx(col(Columns.Hours), lit(0.75), lit(10000)).as(Columns.P75Hours),
    percentile_approx(col(Columns.Hours), lit(0.9), lit(10000)).as(Columns.P90Hours),
    percentile_approx(col(Columns.Hours), lit(0.95), lit(10000)).as(Columns.P95Hours)
  )

  /** One K-anonymous distribution of created-to-hired hours from unexpired stage times. Hours come from the shared
    * eligibility rule, `eligibleCount` counts distinct candidate subjects, and `excludedCount` counts the subjects that
    * have an application outside that rule.
    */
  def timeToHireAction[F[_]: Async](
      stageTimes: DataFrame,
      sparkExecution: SparkExecution[F]
  ): F[DataFrame] = sparkExecution.either(validateStageTimes(stageTimes)).flatMap { rows =>
    sparkExecution {
      val eligible = eligibleDurations(rows, AnalyticsHiringStageMetric.TimeToHire)
      val eligibleSubjects = eligible.select(Columns.SubjectToken).distinct().count()
      val excludedSubjects = rows
        .join(eligible.select(Columns.ApplicationId).distinct(), Seq(Columns.ApplicationId), "left_anti")
        .select(Columns.SubjectToken)
        .distinct()
        .count()
      eligible
        .agg(percentiles().head, percentiles().tail*)
        .withColumn(Columns.EligibleCount, lit(eligibleSubjects))
        .withColumn(Columns.ExcludedCount, lit(excludedSubjects))
        .filter(
          col(Columns.EligibleCount) >= lit(AnalyticsRetention.MinimumContributors) &&
            (col(Columns.ExcludedCount) === lit(0) || col(Columns.ExcludedCount) >= lit(
              AnalyticsRetention.MinimumContributors
            ))
        )
    }
  }

  /** One row per hiring stage metric with at least K distinct candidate subjects; other metrics are omitted. */
  def stageDurations(stageTimes: DataFrame): Either[AnalyticsError, DataFrame] =
    validateStageTimes(stageTimes).map { rows =>
      AnalyticsHiringStageMetric.values.toVector
        .map { metric =>
          eligibleDurations(rows, metric)
            .agg(
              percentiles().head,
              percentiles().tail :+ countDistinct(col(Columns.SubjectToken)).as(Columns.EligibleCount)*
            )
            .withColumn(Columns.Metric, lit(metric.wire))
        }
        .reduce(_.unionByName(_))
        .filter(col(Columns.EligibleCount) >= lit(AnalyticsRetention.MinimumContributors))
        .select(AnalyticsTableSchemas.stageDurations.map { case (name, _) => col(name) }*)
    }

  private def validateStageTimes(stageTimes: DataFrame): Either[AnalyticsError, DataFrame] =
    Either.cond(
      AnalyticsTableSchemas.matches(stageTimes.schema, AnalyticsTableSchemas.stageTimes),
      stageTimes,
      AnalyticsError.InvalidStageTimesSchema
    )
}
