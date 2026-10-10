package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsApplicationStatus
import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.AnalyticsRetention
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{
  col,
  count,
  countDistinct,
  date_trunc,
  explode,
  length,
  lit,
  lower,
  min,
  percentile_approx,
  trim,
  unix_timestamp,
  when
}

object HiringGoldTransforms {
  private val ApplicationCreated = AnalyticsEventType.ApplicationCreated.wire
  private val ApplicationStatusChanged = AnalyticsEventType.ApplicationStatusChanged.wire
  private val JobCreated = AnalyticsEventType.JobCreated.wire
  private val Hired = AnalyticsApplicationStatus.Hired.wire

  private def applicationLifecycle(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms
      .validateSilverSchema(silver)
      .map(_.filter(col(Columns.EventType).isin(ApplicationCreated, ApplicationStatusChanged)))

  /** The CANDIDATE_HIRED event duplicates the Hired status transition and is deliberately excluded. */
  def funnelActivity(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    applicationLifecycle(silver).map { validated =>
      validated
        .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
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
      .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
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
        .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
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

  /** One K-anonymous distribution, with hours calculated only from application lifecycle events. */
  def timeToHireAction[F[_]: Async](
      silver: DataFrame,
      sparkExecution: SparkExecution[F]
  ): F[DataFrame] = sparkExecution.either(applicationLifecycle(silver)).flatMap { valid =>
    sparkExecution {
      val lifecycle = valid
        .groupBy(Columns.ApplicationId, Columns.SubjectToken)
        .agg(
          min(when(col(Columns.EventType) === lit(ApplicationCreated), col(Columns.OccurredAt))).as(Columns.CreatedAt),
          min(when(col(Columns.NewStatus) === lit(Hired), col(Columns.OccurredAt))).as(Columns.HiredAt)
        )
      val eligible = lifecycle
        .filter(col(Columns.CreatedAt).isNotNull && col(Columns.HiredAt).isNotNull)
        .withColumn(
          Columns.Hours,
          (unix_timestamp(col(Columns.HiredAt)) - unix_timestamp(col(Columns.CreatedAt))) / lit(3600.0)
        )
        .filter(col(Columns.Hours) >= lit(0.0))
      val eligibleSubjects = eligible.select(Columns.SubjectToken).distinct().count()
      val excludedSubjects = lifecycle
        .join(eligible.select(Columns.ApplicationId).distinct(), Seq(Columns.ApplicationId), "left_anti")
        .select(Columns.SubjectToken)
        .distinct()
        .count()
      val result = eligible
        .agg(
          percentile_approx(col(Columns.Hours), lit(0.5), lit(10000)).as(Columns.P50Hours),
          percentile_approx(col(Columns.Hours), lit(0.75), lit(10000)).as(Columns.P75Hours),
          percentile_approx(col(Columns.Hours), lit(0.9), lit(10000)).as(Columns.P90Hours),
          percentile_approx(col(Columns.Hours), lit(0.95), lit(10000)).as(Columns.P95Hours),
          count(lit(1)).as(Columns.EligibleApplications)
        )
        .withColumn(Columns.EligibleCount, lit(eligibleSubjects))
        .withColumn(Columns.ExcludedCount, lit(excludedSubjects))
        .filter(
          col(Columns.EligibleCount) >= lit(AnalyticsRetention.MinimumContributors) &&
            (col(Columns.ExcludedCount) === lit(0) || col(Columns.ExcludedCount) >= lit(
              AnalyticsRetention.MinimumContributors
            ))
        )
        .drop(Columns.EligibleApplications)
      result
    }
  }
}
