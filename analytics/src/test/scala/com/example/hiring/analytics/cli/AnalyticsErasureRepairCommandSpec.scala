package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.{ErasureFailureCategory, ErasurePhase}
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
    val receiptId = UUID.randomUUID().toString
    assertEquals(
      command.parse(Seq("requeue", receiptId, "3")),
      Right(AnalyticsErasureRepairMain.RepairAction.Requeue(receiptId, 3))
    )
  }

  test("AEC-06.2 inspect output shows the receipt, phase, attempts and category and never a subject UUID") {
    val line = AnalyticsErasureRepairMain.inspectionLine(
      Some("receipt-4f2a"),
      ErasurePhase.DeltaPurged,
      3,
      ErasureFailureCategory.TransientStorage
    )
    assertEquals(line, "receipt=receipt-4f2a phase=DeltaPurged attempts=3 category=TRANSIENT_STORAGE")
    assert(!line.contains("request="), clue(line))
    val uuid = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r
    assert(uuid.findFirstIn(line).isEmpty, clue(line))
    assertEquals(
      AnalyticsErasureRepairMain.inspectionLine(None, ErasurePhase.Requested, 1, ErasureFailureCategory.Unknown),
      "receipt=none phase=Requested attempts=1 category=UNKNOWN"
    )
  }

  test("repair argument errors and help do not expose invalid request identities") {
    assert(command.parse(Seq("inspect", "0")).isLeft)
    assert(command.parse(Seq("inspect", "not-an-int")).isLeft)
    assert(command.parse(Seq("requeue", UUID.randomUUID().toString, "-1")).isLeft)
    val secretLikeId = "credential=bearing/request-id"
    val failure = command.parse(Seq("requeue", secretLikeId, "1")).swap.toOption.get.toString
    assert(failure.contains("invalid repair request identity"))
    assert(!failure.contains(secretLikeId))
    assert(command.parse(Seq("requeue", "a" * 65, "1")).isLeft)
    assert(command.parse(Seq("--help")).isLeft)
  }

  test("AEC-06.2 requeue takes the receipt printed by inspect and its parse result never carries a subject identity") {
    val receipt = "receipt-4f2a"
    val inspected = AnalyticsErasureRepairMain.inspectionLine(
      Some(receipt),
      ErasurePhase.Requested,
      2,
      ErasureFailureCategory.Unknown
    )
    val printedReceipt = inspected.stripPrefix("receipt=").takeWhile(_ != ' ')
    assertEquals(
      command.parse(Seq("requeue", printedReceipt, "2")),
      Right(AnalyticsErasureRepairMain.RepairAction.Requeue(receipt, 2))
    )
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
