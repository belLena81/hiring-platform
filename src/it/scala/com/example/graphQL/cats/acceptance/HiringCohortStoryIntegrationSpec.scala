package com.example.graphQL.cats.acceptance

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.Filters
import io.circe.Json
import io.circe.syntax.*
import scala.concurrent.duration.*

/** The hired-cohort dataset (docs/specs/hiring-end-to-end-acceptance.md, "Hired-cohort dataset") driven over HTTP
  * against the real `Main`: 60 candidates, one application each, in six lifecycle paths of ten candidates.
  *
  * It exists so that every stage the analytics report shows has at least ten distinct candidates. The expected funnel
  * is checked on the report (analytics-build suite and Admin suite), not here; this suite proves that the lifecycle was
  * committed as the table says, publishes the events and exports them for the analytics step.
  */
class HiringCohortStoryIntegrationSpec extends HiringStoryStackSuite {
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private enum Path(val steps: Vector[(String, Json)], val finalStatus: String) {
    case Hired
        extends Path(
          Vector(
            "acceptApplication" -> Json.obj(),
            "moveApplicationToInterview" -> Json.obj(),
            "hireApplication" -> Json.obj()
          ),
          "HIRED"
        )
    case RejectedAfterInterview
        extends Path(
          Vector(
            "acceptApplication" -> Json.obj(),
            "moveApplicationToInterview" -> Json.obj(),
            "rejectApplication" -> Json.obj("feedback" -> "Not a fit after the interview.".asJson)
          ),
          "REJECTED"
        )
    case RejectedUnseen
        extends Path(
          Vector("rejectApplication" -> Json.obj("feedback" -> "Profile does not match.".asJson)),
          "REJECTED"
        )
    case Declined
        extends Path(
          Vector("declineApplication" -> Json.obj("reason" -> "I accepted another offer.".asJson)),
          "DECLINED"
        )
    case AcceptedOnly extends Path(Vector("acceptApplication" -> Json.obj()), "ACCEPTED")
    case CreatedOnly extends Path(Vector.empty, "CREATED")
  }

  private val perPath = 10
  private val candidatesTotal = Path.values.length * perPath

  private val conflicts = new java.util.concurrent.atomic.AtomicInteger(0)

  /** A lost concurrent write answers CONFLICT, which `docs/api.md` says to retry with the same idempotency key. */
  private def withRetry(
      name: String,
      actor: StoryActor,
      application: String,
      extra: Json,
      candidate: Int
  ): IO[StoryOutcome] = {
    val key = operations.key
    val input = Json.obj("applicationId" -> application.asJson).deepMerge(extra)
    def attempt(remaining: Int): IO[StoryOutcome] =
      operations.sendWithKey(name, input, Some(actor), key).map(StoryOutcome.of(_, name)).flatMap {
        case StoryOutcome.Refused("CONFLICT", _, _) if remaining > 0 =>
          IO(println(s"HEA-12.0 CONFLICT retry: $name candidate $candidate attempt ${6 - remaining} of 5")) *> IO(
            conflicts.incrementAndGet()
          ) *> IO.sleep(200.millis) *> attempt(remaining - 1)
        case other => IO.pure(other)
      }
    attempt(5)
  }

  private def unwrap(outcome: StoryOutcome, what: String): Json = outcome match {
    case StoryOutcome.Succeeded(value) => value
    case other                         => fail(s"$what expected success but was $other")
  }

  test("HEA-12.0 sixty candidates apply to one job and move through the six lifecycle paths") {
    val started = System.nanoTime()
    for {
      recruiter <- operations.signUpRecruiter("Rhea Cohort", "Cohort Hiring")
      job <- operations.createPublishedJob(recruiter, "Cohort Backend", "Berlin", List("Scala"))
      candidates <- (1 to candidatesTotal).toVector.traverse(i =>
        operations.signUpCandidate(f"Cohort Candidate $i%02d", List("Scala")).map(i -> _)
      )
      applications <- candidates.traverse { case (i, actor) =>
        operations.submitOk(actor, job).map(application => (i, actor, application))
      }
      _ <- applications.traverse_ { case (i, actor, application) =>
        val path = Path.fromOrdinal((i - 1) / perPath)
        path.steps.traverse_ { case (name, extra) =>
          // The declining path acts as the candidate who owns the application; all other steps as the recruiter.
          val by = if (name == "declineApplication") actor else recruiter
          withRetry(name, by, application, extra, i).map(unwrap(_, s"$name for candidate $i"))
        }
      }
      counts <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> job.asJson), recruiter)
    } yield {
      val byStatus = counts.hcursor
        .downField("applicationCounts")
        .downField("byStatus")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .flatMap(entry =>
          (entry.hcursor.get[String]("status"), entry.hcursor.get[Int]("count")).tupled.toOption.filter(_._2 > 0)
        )
        .toMap
      val elapsed = (System.nanoTime() - started) / 1000000000L
      // 61 sign-ups, create and publish (2), 60 submissions, 90 status changes: 213 mutations.
      println(
        s"HEA-12.0 cohort: $candidatesTotal candidates, 213 mutations, elapsed ${elapsed}s, CONFLICT retries ${conflicts.get}, statuses $byStatus"
      )
      // Final statuses from the spec table: P1 hired 10; P2 and P3 rejected 20; P4 declined 10; P5 accepted 10; P6 created 10.
      assertEquals(
        byStatus,
        Map("HIRED" -> 10, "REJECTED" -> 20, "DECLINED" -> 10, "ACCEPTED" -> 10, "CREATED" -> 10)
      )
    }
  }

  private def outboxPending: IO[Long] =
    stack.database.getCollection("event_outbox").flatMap(_.count(Filters.ne("state", "Published")))

  private def outboxTotal: IO[Long] = stack.database.getCollection("event_outbox").flatMap(_.count)

  private def untilPublished: IO[Unit] =
    (outboxPending, outboxTotal).tupled.flatMap { case (pending, total) =>
      if (pending == 0L && total > 0L) IO.unit else IO.sleep(250.millis) *> untilPublished
    }

  test("HEA-11.3 the cohort's published events are exported for the analytics step") {
    AnalyticsChain.require()
    for {
      _ <- untilPublished.timeout(120.seconds)
      exported <- HiringStoryEventExport.write(
        stack.running.namespace,
        "cohort-events.jsonl",
        "cohort-report-documents.jsonl"
      )
    } yield {
      // 61 jobs-and-applications creations (1 job, 60 applications), 90 status changes, 10 CANDIDATE_HIRED.
      println(s"HEA-11.3 exported ${exported._2} cohort events")
      assertEquals(exported._2, 1 + 60 + 90 + 10)
    }
  }
}
