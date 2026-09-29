package com.example.hiring.analytics.cli

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.monovore.decline.Command
import munit.FunSuite

import java.util.UUID

final class AnalyticsErasureRepairCommandSpec extends FunSuite {
  private val command = Command("analytics-erasure-repair", "Inspect and requeue analytics erasure repair requests")(
    AnalyticsErasureRepairMain.repairOptions
  )

  test("inspect and requeue retain their positional arguments") {
    assertEquals(
      command.parse(Seq("inspect", "25")),
      Right(AnalyticsErasureRepairMain.RepairAction.Inspect(25))
    )
    val requestId = UUID.randomUUID().toString
    assertEquals(
      command.parse(Seq("requeue", requestId, "3")),
      Right(AnalyticsErasureRepairMain.RepairAction.Requeue(AccountSubjectId.from(requestId).toOption.get, 3))
    )
  }

  test("repair argument errors and help do not expose invalid request identities") {
    assert(command.parse(Seq("inspect", "0")).isLeft)
    assert(command.parse(Seq("inspect", "not-an-int")).isLeft)
    assert(command.parse(Seq("requeue", UUID.randomUUID().toString, "-1")).isLeft)
    val secretLikeId = "credential-bearing-request-id"
    val failure = command.parse(Seq("requeue", secretLikeId, "1")).swap.toOption.get.toString
    assert(failure.contains("invalid repair request identity"))
    assert(!failure.contains(secretLikeId))
    assert(command.parse(Seq("--help")).isLeft)
  }

  test("CLI error summaries include safe cause types without raw exception details") {
    val cause = new IllegalArgumentException("credential=do-not-log")
    val error = AnalyticsError.MongoConnectionFailure(cause)
    val summary = AnalyticsCliProgram.safeFailureSummary(error)
    assert(summary.contains("analytics Mongo client could not start"))
    assert(summary.contains("IllegalArgumentException"))
    assert(!summary.contains("credential=do-not-log"))
    val unexpected = AnalyticsCliProgram.safeFailureSummary(cause)
    assert(unexpected.contains("IllegalArgumentException"))
    assert(!unexpected.contains("credential=do-not-log"))
    val dynamic = AnalyticsCliProgram.safeFailureSummary(AnalyticsError.RunIdRangeConflict("private-run-id"))
    assert(dynamic.contains("analytics run ID conflicts"))
    assert(!dynamic.contains("private-run-id"))
  }
}
