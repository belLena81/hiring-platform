package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsEventTimePolicy, EventTimeAdmission}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.*

import java.time.Instant

/** Built-in Spark expression form of `AnalyticsEventTimePolicy.admit`.
  *
  * Spark timestamps have microsecond precision, so all arithmetic is on epoch microseconds (`Long`, no floating point,
  * no session time zone: UTC days are `pmod` of a fixed 86 400 000 000 microseconds). The two instants taken from the
  * runtime are floored to microseconds, which preserves every comparison against microsecond event times. A null event
  * time is classified as `FUTURE`, as before. `EventTimeAdmissionExpressionSpec` proves equality with the policy.
  */
private[spark] object EventTimeAdmissionExpression {
  val StatusField = "status"
  val EffectiveTimeField = "effectiveTime"

  private val MicrosPerDay = 86400L * 1000000L

  def epochMicros(instant: Instant): Long =
    Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), (instant.getNano / 1000).toLong)

  /** A struct column `(status, effectiveTime)`; `effectiveTime` is set only for `OPEN`. */
  def classify(eventTime: Column, observedAt: Instant, previousWatermark: Option[Instant]): Column = {
    val micros = unix_micros(eventTime)
    val observedMicros = epochMicros(observedAt)
    val futureLimit = observedMicros + AnalyticsEventTimePolicy.AllowedFutureSkew.toMicros
    val dayEnd = micros - pmod(micros, lit(MicrosPerDay)) + lit(MicrosPerDay)
    val tooFarInFuture = eventTime.isNull || micros > lit(futureLimit)
    val closedDay = previousWatermark.map(epochMicros) match {
      case Some(watermark) => dayEnd <= lit(watermark)
      case None            => lit(false)
    }
    val status = when(tooFarInFuture, lit(EventTimeAdmission.FutureStatus))
      .when(closedDay, lit(EventTimeAdmission.ClosedStatus))
      .otherwise(lit(EventTimeAdmission.OpenStatus))
    val effective = when(
      !tooFarInFuture && !closedDay,
      timestamp_micros(least(micros, lit(observedMicros)))
    )
    struct(status.as(StatusField), effective.as(EffectiveTimeField))
  }
}
