package com.example.hiring.analytics

import cats.Applicative
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.syntax.all.*
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.StreamingBatchCost
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehouseLock,
  AnalyticsReportPublicationReceipt,
  AnalyticsReportReservation
}
import com.example.hiring.analytics.service.streaming.*
import munit.CatsEffectSuite
import java.time.Instant
import scala.concurrent.duration.*

final class StreamingBatchCostSpec extends CatsEffectSuite {
  private def right[A](value: Either[String, A]): A = value.fold(problem => fail(problem), result => result)
  private val preparation = StreamingInputPreparation(
    StreamingBatchIdentity(right(StreamingLineage.from("cost-test")), right(StreamingBatchId.from(1L))),
    Instant.EPOCH,
    None,
    right(RangeFingerprint.from("a" * 64)),
    Vector.empty,
    Vector.empty
  )
  private val reservation =
    AnalyticsReportReservation(right(RunId.from("cost-test")), preparation.inputFingerprint, 0L, 1L)
  private val decision = StreamingDecisionRevision(preparation.identity, 7L, "marker", None, reservation)
  private val assessment = StreamingIngestionResult.Ready(Vector(Instant.EPOCH))

  private def stages(action: IO[Unit]): StreamingBatchStages[IO] = new StreamingBatchStages[IO] {
    override def reservePublication(p: StreamingInputPreparation, revision: Long): IO[AnalyticsReportReservation] =
      IO { assertEquals(p, preparation); assertEquals(revision, 7L) } *> action.as(reservation)
    override def publicationReceipt(p: StreamingInputPreparation, d: StreamingDecisionRevision)(using
        Applicative[IO]
    ): IO[AnalyticsReportPublicationReceipt] =
      IO { assertEquals(p, preparation); assertEquals(d, decision) } *> action.as(
        AnalyticsReportPublicationReceipt.CurrentGeneration
      )
    override def assess(
        p: StreamingInputPreparation,
        tokens: Vector[SubjectToken],
        recovery: Boolean
    ): IO[StreamingIngestionResult] =
      IO { assertEquals(p, preparation); assertEquals(tokens, Vector.empty); assert(recovery) } *> action.as(assessment)
    override def ingest(
        p: StreamingInputPreparation,
        tokens: Vector[SubjectToken],
        a: StreamingIngestionResult,
        d: StreamingDecisionRevision,
        recovery: Boolean
    ): IO[Unit] =
      IO {
        assertEquals(p, preparation); assertEquals(tokens, Vector.empty); assertEquals(a, assessment);
        assertEquals(d, decision); assert(recovery)
      } *> action
    override def publish(
        p: StreamingInputPreparation,
        d: StreamingDecisionRevision,
        tokens: Vector[SubjectToken]
    ): IO[StreamingPublicationResult] =
      IO { assertEquals(p, preparation); assertEquals(d, decision); assertEquals(tokens, Vector.empty) } *> action.as(
        StreamingPublicationResult.Published
      )
  }

  test("diagnostics preserve every stage argument and result and measure bounded counter differences") {
    TestControl.executeEmbed {
      for {
        counter <- Ref.of[IO, StreamingBatchCost.Counters](StreamingBatchCost.Counters())
        summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
        cost = new StreamingBatchCost[IO](counter.get, value => summaries.update(_ :+ value))
        wrapped = cost.wrap(stages(IO.sleep(2.seconds) *> counter.update(v => v.copy(tasks = v.tasks + 3L))))
        r <- wrapped.reservePublication(preparation, 7L)
        receipt <- wrapped.publicationReceipt(preparation, decision)
        a <- wrapped.assess(preparation, Vector.empty, true)
        _ <- wrapped.ingest(preparation, Vector.empty, assessment, decision, true)
        p <- wrapped.publish(preparation, decision, Vector.empty)
        recorded <- summaries.get
        _ <- IO {
          assertEquals(r, reservation)
          assertEquals(receipt, AnalyticsReportPublicationReceipt.CurrentGeneration)
          assertEquals(a, assessment)
          assertEquals(p, StreamingPublicationResult.Published)
          assertEquals(
            recorded.map(_.stage),
            Vector(
              StreamingBatchCost.Stage.Reserve,
              StreamingBatchCost.Stage.Receipt,
              StreamingBatchCost.Stage.Assess,
              StreamingBatchCost.Stage.Ingest,
              StreamingBatchCost.Stage.Publish
            )
          )
          assert(
            recorded.forall(v =>
              v.result == StreamingBatchCost.Result.Succeeded && v.elapsed == 2.seconds && v.work.tasks == 3L
            )
          )
        }
      } yield ()
    }
  }

  test("diagnostic failure cannot mask stage failure") {
    val failure = new IllegalStateException("stage failure")
    for {
      summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
      cost = new StreamingBatchCost[IO](
        IO.pure(StreamingBatchCost.Counters()),
        value => summaries.update(_ :+ value) *> IO.raiseError(new Exception("observer"))
      )
      result <- cost.wrap(stages(IO.raiseError(failure))).publish(preparation, decision, Vector.empty).attempt
      recorded <- summaries.get
      _ <- IO {
        assertEquals(result, Left(failure))
        assertEquals(recorded.map(_.result), Vector(StreamingBatchCost.Result.Failed))
      }
    } yield ()
  }

  test("cancellation reaches the wrapped stage and emits a sanitized terminal summary") {
    for {
      entered <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
      cost = new StreamingBatchCost[IO](IO.pure(StreamingBatchCost.Counters()), value => summaries.update(_ :+ value))
      wrapped = cost.wrap(stages((entered.complete(()).void *> IO.never[Unit]).onCancel(released.set(true))))
      _ <- wrapped.publish(preparation, decision, Vector.empty).background.use(_ => entered.get)
      wasReleased <- released.get
      recorded <- summaries.get
      _ <- IO {
        assert(wasReleased)
        assertEquals(recorded.map(_.result), Vector(StreamingBatchCost.Result.Cancelled))
        assert(!recorded.toString.contains("cost-test"))
      }
    } yield ()
  }

  test("mutex diagnostics measure acquisition and preserve cancellation-safe release") {
    TestControl.executeEmbed {
      for {
        held <- Ref.of[IO, Boolean](false)
        summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
        delegate = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.make(IO.sleep(2.seconds) *> held.set(true))(_ => held.set(false))
        }
        cost = new StreamingBatchCost[IO](IO.pure(StreamingBatchCost.Counters()), value => summaries.update(_ :+ value))
        _ <- cost
          .wrapLock(delegate)
          .resource("hiring")
          .use(_ => held.get.flatMap(v => IO(assert(v))) *> IO.sleep(10.seconds))
        released <- held.get
        observed <- summaries.get
        _ <- IO {
          assertEquals(released, false)
          assertEquals(observed.map(_.elapsed), Vector(2.seconds))
          assertEquals(observed.map(_.stage), Vector(StreamingBatchCost.Stage.MutexWait))
        }
        entered <- Deferred[IO, Unit]
        queued = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.eval(entered.complete(()).void *> IO.never[Unit])
        }
        _ <- cost.wrapLock(queued).resource("hiring").use(_ => IO.unit).background.use(_ => entered.get)
        cancelled <- summaries.get
        _ <- IO(assertEquals(cancelled.lastOption.map(_.result), Some(StreamingBatchCost.Result.Cancelled)))
      } yield ()
    }
  }

  private def journal(action: String => IO[Unit]): StreamingBatchJournal[IO] = new StreamingBatchJournal[IO] {
    override def load(identity: StreamingBatchIdentity): IO[Option[StreamingJournalState]] =
      IO(assertEquals(identity, preparation.identity)) *> action("load").as(None)
    override def latestWatermark(lineage: StreamingLineage): IO[Option[Instant]] =
      IO(assertEquals(lineage, preparation.identity.lineage)) *> action("watermark").as(Some(Instant.EPOCH))
    override def hasLineageState(lineage: StreamingLineage): IO[Boolean] =
      IO(assertEquals(lineage, preparation.identity.lineage)) *> action("lineage").as(true)
    override def reconciliationStates(
        lineage: StreamingLineage,
        retained: Set[StreamingBatchId]
    ): IO[Vector[StreamingJournalState]] =
      IO {
        assertEquals(lineage, preparation.identity.lineage); assertEquals(retained, Set(preparation.identity.batchId))
      } *>
        action("reconciliation").as(Vector.empty)
    override def prepare(value: StreamingInputPreparation): IO[Unit] =
      IO(assertEquals(value, preparation)) *> action("prepare")
    override def markIngestionCommitted(identity: StreamingBatchIdentity): IO[Unit] =
      IO(assertEquals(identity, preparation.identity)) *> action("ingestion")
    override def appendDecision(value: StreamingDecisionRevision): IO[Unit] =
      IO(assertEquals(value, decision)) *> action("decision")
    override def complete(identity: StreamingBatchIdentity, outcome: StreamingTerminalOutcome, at: Instant): IO[Unit] =
      IO {
        assertEquals(identity, preparation.identity); assertEquals(outcome, StreamingTerminalOutcome.Published);
        assertEquals(at, Instant.EPOCH)
      } *> action("complete")
    override def commitPublished(value: StreamingDecisionRevision, at: Instant): IO[Unit] =
      IO { assertEquals(value, decision); assertEquals(at, Instant.EPOCH) } *> action("publication")
  }

  test("optional journal diagnostics preserve all typed operations exactly once and None keeps delegate") {
    def run(enabled: Boolean): IO[(Vector[String], Vector[StreamingBatchCost.Stage])] = for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
      delegate = journal(name => calls.update(_ :+ name))
      cost = new StreamingBatchCost[IO](
        IO.pure(StreamingBatchCost.Counters()),
        summary => summaries.update(_ :+ summary)
      )
      option = Option.when(enabled)(cost)
      observed = option.fold(delegate)(_.wrapJournal(delegate))
      _ <- IO(if (!enabled) assert(observed eq delegate))
      loaded <- observed.load(preparation.identity)
      watermark <- observed.latestWatermark(preparation.identity.lineage)
      lineage <- observed.hasLineageState(preparation.identity.lineage)
      reconciled <- observed.reconciliationStates(preparation.identity.lineage, Set(preparation.identity.batchId))
      _ <- observed.prepare(preparation)
      _ <- observed.markIngestionCommitted(preparation.identity)
      _ <- observed.appendDecision(decision)
      _ <- observed.complete(preparation.identity, StreamingTerminalOutcome.Published, Instant.EPOCH)
      _ <- observed.commitPublished(decision, Instant.EPOCH)
      _ <- IO {
        assertEquals(loaded, None); assertEquals(watermark, Some(Instant.EPOCH)); assert(lineage);
        assertEquals(reconciled, Vector.empty)
      }
      recorded <- calls.get
      stages <- summaries.get.map(_.map(_.stage))
    } yield (recorded, stages)
    for {
      normal <- run(false)
      diagnostic <- run(true)
      _ <- IO {
        assertEquals(
          normal._1,
          Vector(
            "load",
            "watermark",
            "lineage",
            "reconciliation",
            "prepare",
            "ingestion",
            "decision",
            "complete",
            "publication"
          )
        )
        assertEquals(diagnostic._1, normal._1)
        assertEquals(normal._2, Vector.empty)
        assertEquals(
          diagnostic._2,
          Vector(
            StreamingBatchCost.Stage.JournalLoad,
            StreamingBatchCost.Stage.JournalWatermark,
            StreamingBatchCost.Stage.JournalLineage,
            StreamingBatchCost.Stage.JournalReconciliation,
            StreamingBatchCost.Stage.JournalPrepare,
            StreamingBatchCost.Stage.JournalIngestion,
            StreamingBatchCost.Stage.JournalDecision,
            StreamingBatchCost.Stage.JournalComplete,
            StreamingBatchCost.Stage.JournalPublication
          )
        )
      }
    } yield ()
  }

  test("journal diagnostic error and cancellation preserve delegate ownership and sanitized summaries") {
    val failure = new IllegalStateException("private failure")
    for {
      summaries <- Ref.of[IO, Vector[StreamingBatchCost.Summary]](Vector.empty)
      calls <- Ref.of[IO, Int](0)
      cost = new StreamingBatchCost[IO](IO.pure(StreamingBatchCost.Counters()), value => summaries.update(_ :+ value))
      failed <- cost
        .wrapJournal(journal(_ => calls.update(_ + 1) *> IO.raiseError(failure)))
        .prepare(preparation)
        .attempt
      _ <- IO(assertEquals(failed, Left(failure)))
      entered <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      action = (calls.update(_ + 1) *> entered.complete(()).void *> IO.never[Unit]).onCancel(released.set(true))
      _ <- cost.wrapJournal(journal(_ => action)).load(preparation.identity).background.use(_ => entered.get)
      count <- calls.get
      closed <- released.get
      recorded <- summaries.get
      _ <- IO {
        assertEquals(count, 2)
        assert(closed)
        assertEquals(
          recorded.map(_.result),
          Vector(StreamingBatchCost.Result.Failed, StreamingBatchCost.Result.Cancelled)
        )
        assert(!recorded.toString.contains("private failure"))
        assert(!recorded.toString.contains("cost-test"))
      }
    } yield ()
  }

}
