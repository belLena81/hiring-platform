package com.example.graphQL.cats.acceptance

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.Filters
import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*

/** The hiring story (docs/specs/hiring-end-to-end-acceptance.md, Tier A) driven over HTTP against the real `Main`.
  *
  * The tests run in the order of the story and share its state: accounts, jobs and applications created by an earlier
  * test are read by the later ones. Expected values come from the spec's story table, never from the code under test.
  * Atlas-dependent (HEA-02, 06, 10) and analytics (HEA-12, 13) criteria are not exercised here.
  */
class HiringStoryAcceptanceIntegrationSpec extends HiringStoryStackSuite {
  // Sequential story state; tests of one suite never run concurrently (IntegrationTest / parallelExecution is off).
  private val actors = TrieMap.empty[String, StoryActor]
  private val ids = TrieMap.empty[String, String]

  private val candidateNames = Vector(
    "Ana Rivera",
    "Bo Lindqvist",
    "Cy Okafor",
    "Dana Weiss",
    "Eli Moreau",
    "Fay Novak",
    "Gus Tanaka",
    "Hana Petrov",
    "Ivo Santos",
    "Jun Almeida",
    "Kai Brandt",
    "Lia Costa"
  )
  private def candidate(index: Int): StoryActor = actors(f"C$index%02d")
  private def rita: StoryActor = actors("R1")
  private def ravi: StoryActor = actors("R2")
  private def application(index: Int): String = ids(f"A$index%02d")
  private val outboxCollection = "event_outbox"

  private def asString(value: Json, field: String): String =
    value.hcursor.get[String](field).fold(e => fail(e.message), identity)

  private def nodeIds(connection: Json): Vector[String] = operations.edges(connection).map(operations.idOf)

  private def refusalCode(outcome: StoryOutcome): String = outcome match {
    case StoryOutcome.Refused(code, _, _) => code
    case other                            => fail(s"expected a refusal but was $other")
  }

  private def historyChain(events: Vector[Json]): Vector[(Option[String], String)] =
    events.map(event => (event.hcursor.get[String]("previousStatus").toOption, asString(event, "newStatus")))

  /** Orders history by its chain of previous/new statuses, oldest first, regardless of the page order. */
  private def chained(events: Vector[Json]): Vector[Json] = {
    def from(previous: Option[String], remaining: Vector[Json]): Vector[Json] =
      remaining.find(_.hcursor.get[String]("previousStatus").toOption == previous) match {
        case Some(next) =>
          next +: from(Some(asString(next, "newStatus")), remaining.filterNot(_ eq next))
        case None => Vector.empty
      }
    from(None, events)
  }

  private def jobsFor(actor: StoryActor, filter: Json, first: Int, after: Option[String] = None): IO[Json] =
    operations.read(
      StoryDocuments.jobs,
      "jobs",
      Json.obj("filter" -> filter, "first" -> first.asJson, "after" -> after.asJson),
      actor
    )

  private def allOpenJobs(actor: StoryActor): IO[Vector[String]] =
    jobsFor(actor, Json.Null, 20).map(nodeIds)

  private def applicationStatuses(jobId: String): IO[Map[String, String]] =
    operations
      .read(
        StoryDocuments.jobApplications,
        "jobApplications",
        Json.obj("jobId" -> jobId.asJson, "first" -> 100.asJson),
        rita
      )
      .map(connection =>
        operations.edges(connection).map(node => operations.idOf(node) -> asString(node, "status")).toMap
      )

  test(
    "HEA-00.1 the Admin is seeded through configuration and signs in; accounts sign up; there is no public Admin bootstrap"
  ) {
    for {
      admin <- adminActor
      attempt <- operations.send(
        "signUp",
        Json.obj(
          "name" -> "Second Admin".asJson,
          "role" -> "ADMIN".asJson,
          "password" -> "second-admin-password-1".asJson
        ),
        None
      )
      r1 <- operations.signUpRecruiter("Rita Okonkwo", "Northwind Hiring")
      r2 <- operations.signUpRecruiter("Ravi Shankar", "Contoso Talent")
      cs <- candidateNames.zipWithIndex.traverse { case (name, index) =>
        operations.signUpCandidate(name, if (index == 0) List("Scala", "Kafka") else List("Scala"))
      }
      me <- operations.read(StoryDocuments.me, "me", Json.obj(), admin)
    } yield {
      assertEquals(asString(me, "role"), "ADMIN")
      assertEquals(refusalCode(StoryOutcome.of(attempt, "signUp")), "ADMIN_BOOTSTRAP_ONLY")
      actors.put("ADMIN", admin)
      actors.put("R1", r1)
      actors.put("R2", r2)
      cs.zipWithIndex.foreach { case (actor, index) => actors.put(f"C${index + 1}%02d", actor) }
      assertEquals(actors.size, 15)
    }
  }

  test("HEA-07.3 recruiters create and publish the story jobs, R1 updates J3, and publishing an open job is refused") {
    for {
      j1 <- operations.createPublishedJob(rita, "Scala Backend", "Berlin", List("Scala", "Kafka"))
      j2 <- operations.createPublishedJob(rita, "Kotlin Services", "Berlin", List("Kotlin"))
      j3 <- operations.createPublishedJob(rita, "Java Developer", "Munich", List("Java"))
      j4 <- operations.createPublishedJob(ravi, "Data Engineer", "Berlin", List("Spark", "SQL"))
      updated <- operations.succeed(
        "updateJob",
        Json.obj("id" -> j3.asJson, "job" -> operations.jobInput("Java Developer (Senior)", "Munich", List("Java"))),
        rita
      )
      publish <- operations.mutate("publishJob", Json.obj("jobId" -> j1.asJson), rita)
    } yield {
      ids.put("J1", j1); ids.put("J2", j2); ids.put("J3", j3); ids.put("J4", j4)
      assertEquals(asString(updated, "title"), "Java Developer (Senior)")
      assertEquals(asString(updated, "status"), "OPEN")
      assertEquals(refusalCode(publish), "INVALID_JOB_TRANSITION")
    }
  }

  test("HEA-07.1 R2 cannot update or close J1 and J1 is unchanged") {
    for {
      before <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> ids("J1").asJson), rita)
      update <- operations.mutate(
        "updateJob",
        Json.obj("id" -> ids("J1").asJson, "job" -> operations.jobInput("Hijacked", "Berlin", List("Scala"))),
        ravi
      )
      close <- operations.mutate("closeJob", Json.obj("jobId" -> ids("J1").asJson), ravi)
      after <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> ids("J1").asJson), rita)
    } yield {
      // A job R2 does not own answers as a missing one.
      assertEquals(refusalCode(update), "NOT_FOUND")
      assertEquals(refusalCode(close), "NOT_FOUND")
      assertEquals(after, before)
      assertEquals(asString(after, "status"), "OPEN")
    }
  }

  test("HEA-07.2 when R1 closes J3 it leaves discovery and still lists under myJobs") {
    for {
      before <- jobsFor(candidate(1), Json.obj("city" -> "Munich".asJson), 10).map(nodeIds)
      closed <- operations.succeed("closeJob", Json.obj("jobId" -> ids("J3").asJson), rita)
      after <- jobsFor(candidate(1), Json.obj("city" -> "Munich".asJson), 10).map(nodeIds)
      mine <- operations.read(
        StoryDocuments.myJobs,
        "myJobs",
        Json.obj("first" -> 10.asJson, "status" -> "CLOSED".asJson),
        rita
      )
      j5 <- operations.createJob(rita, "Go Platform Engineer", "Hamburg", List("Go"))
      j5Status <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> j5.asJson), rita)
    } yield {
      ids.put("J5", j5)
      assertEquals(before, Vector(ids("J3")))
      assertEquals(asString(closed, "status"), "CLOSED")
      assertEquals(after, Vector.empty[String])
      assertEquals(nodeIds(mine), Vector(ids("J3")))
      // J5 stays a Draft: created by R1, visible to its owner only, never published in the story.
      assertEquals(asString(j5Status, "status"), "DRAFT")
    }
  }

  test("HEA-01.1 Ana filtering Berlin with the Scala skill gets only J1; pages follow newest first without gaps") {
    val ana = candidate(1)
    for {
      scala <- jobsFor(ana, Json.obj("city" -> "Berlin".asJson, "skills" -> List("Scala").asJson), 10)
      lower <- jobsFor(ana, Json.obj("city" -> " berlin ".asJson, "skills" -> List("scala").asJson), 10)
      pageSizeOne <- fetchPages(ana, Json.obj("city" -> "Berlin".asJson), 1)
      pageSizeTwo <- fetchPages(ana, Json.obj("city" -> "Berlin".asJson), 2)
    } yield {
      ids.put("SEARCH", asString(scala, "searchId"))
      assertEquals(nodeIds(scala), Vector(ids("J1")))
      assertEquals(nodeIds(lower), Vector(ids("J1")))
      val newestFirst = Vector(ids("J4"), ids("J2"), ids("J1"))
      assertEquals(pageSizeOne.map(nodeIds).flatten, newestFirst)
      assertEquals(
        pageSizeOne.map(page => page.hcursor.downField("pageInfo").get[Boolean]("hasNextPage")),
        Vector(Right(true), Right(true), Right(false))
      )
      assertEquals(pageSizeTwo.map(nodeIds), Vector(Vector(ids("J4"), ids("J2")), Vector(ids("J1"))))
    }
  }

  private def fetchPages(actor: StoryActor, filter: Json, size: Int): IO[Vector[Json]] = {
    def next(after: Option[String], collected: Vector[Json]): IO[Vector[Json]] =
      jobsFor(actor, filter, size, after).flatMap { page =>
        val info = page.hcursor.downField("pageInfo")
        val updated = collected :+ page
        if (info.get[Boolean]("hasNextPage") == Right(true) && updated.size < 10)
          next(info.get[String]("endCursor").toOption, updated)
        else IO.pure(updated)
      }
    next(None, Vector.empty)
  }

  test("HEA-01.2 closed J3 and unpublished J5 are hidden from every candidate") {
    List(candidate(1), candidate(2), candidate(3)).traverse(allOpenJobs).map { listings =>
      listings.foreach { listing =>
        assert(!listing.contains(ids("J3")), clues(listing))
        assert(!listing.contains(ids("J5")), clues(listing))
        assert(Set(ids("J1"), ids("J2"), ids("J4")).subsetOf(listing.toSet), clues(listing))
      }
    }
  }

  test("HEA-03.1 Ana opens J1 and records a view: the detail is returned and one view is accepted") {
    val ana = candidate(1)
    val viewEvent = UUID.randomUUID().toString
    val replayKey = operations.key
    val input = Json.obj("eventId" -> viewEvent.asJson, "jobId" -> ids("J1").asJson)
    for {
      detail <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> ids("J1").asJson), ana)
      first <- operations.sendWithKey("recordJobView", input, Some(ana), replayKey)
      replay <- operations.sendWithKey("recordJobView", input, Some(ana), replayKey)
    } yield {
      ids.put("VIEW", viewEvent)
      assertEquals(asString(detail, "title"), "Scala Backend")
      assertEquals(asString(detail, "status"), "OPEN")
      val accepted = StoryOutcome.of(first, "recordJobView")
      assertEquals(
        accepted,
        StoryOutcome.Succeeded(Json.obj("__typename" -> "InteractionSuccess".asJson, "eventId" -> viewEvent.asJson))
      )
      assertEquals(StoryOutcome.of(replay, "recordJobView"), accepted)
    }
  }

  test(
    "HEA-03.2 closed J3 and unpublished J5 answer Ana exactly as an unknown job does; their owner still reads them"
  ) {
    val ana = candidate(1)
    def read(id: String, actor: StoryActor) =
      operations.query(StoryDocuments.job, "job", Json.obj("id" -> id.asJson), Some(actor))
    for {
      closed <- read(ids("J3"), ana)
      draft <- read(ids("J5"), ana)
      unknown <- read(UUID.randomUUID().toString, ana)
      owner <- read(ids("J3"), rita)
      draftOwner <- read(ids("J5"), rita)
      draftByOtherRecruiter <- read(ids("J5"), ravi)
      draftSubmission <- operations.submit(ana, ids("J5"))
      unknownSubmission <- operations.submit(ana, UUID.randomUUID().toString)
    } yield {
      assertEquals(closed, unknown)
      assertEquals(draft, unknown)
      assertEquals(draftByOtherRecruiter, unknown)
      assertEquals(refusalCode(unknown), "NOT_FOUND")
      // Applying to a Draft is refused as for an unknown job, and commits nothing.
      assertEquals(draftSubmission, unknownSubmission)
      owner match {
        case StoryOutcome.Succeeded(value) => assertEquals(asString(value, "status"), "CLOSED")
        case other                         => fail(s"the owner expected to read J3 but was $other")
      }
      draftOwner match {
        case StoryOutcome.Succeeded(value) => assertEquals(asString(value, "status"), "DRAFT")
        case other                         => fail(s"the owner expected to read J5 but was $other")
      }
    }
  }

  test("HEA-04.1 each candidate's submission to J1 creates one Created application with a first history entry") {
    (1 to 12).toList
      .traverse { index =>
        val actor = candidate(index)
        for {
          start <- IO.realTimeInstant
          id <- operations.submitOk(actor, ids("J1"))
          end <- IO.realTimeInstant
          events <- operations.history(actor, id)
        } yield (index, id, start, end, events)
      }
      .map { results =>
        results.foreach { case (index, id, start, end, events) =>
          ids.put(f"A$index%02d", id)
          assertEquals(events.size, 1, clues(index))
          val first = events.head
          assertEquals(first.hcursor.get[String]("previousStatus").toOption, None)
          assertEquals(asString(first, "newStatus"), "CREATED")
          assertEquals(asString(first, "actorRole"), "CANDIDATE")
          val occurredAt = Instant.parse(asString(first, "occurredAt"))
          assert(
            !occurredAt.isBefore(start.minusSeconds(1)) && !occurredAt.isAfter(end.plusSeconds(1)),
            clues(occurredAt, start, end)
          )
        }
        assertEquals(results.map(_._2).distinct.size, 12)
      } *> applicationStatuses(ids("J1")).map { statuses =>
      assertEquals(statuses.size, 12)
      assertEquals(statuses.values.toSet, Set("CREATED"))
    }
  }

  test("HEA-04.2 rejects a second submission by the same candidate and leaves the history unchanged") {
    val ana = candidate(1)
    for {
      outcome <- operations.submit(ana, ids("J1"))
      events <- operations.history(ana, application(1))
      statuses <- applicationStatuses(ids("J1"))
    } yield {
      assertEquals(events.size, 1)
      assertEquals(statuses.size, 12)
      assertEquals(refusalCode(outcome), "DUPLICATE_APPLICATION")
    }
  }

  test("HEA-04.3 two concurrent submissions by Bo to J2 leave exactly one application") {
    val bo = candidate(2)
    for {
      outcomes <- (operations.submit(bo, ids("J2")), operations.submit(bo, ids("J2"))).parTupled
      mine <- operations.read(StoryDocuments.myApplications, "myApplications", Json.obj("first" -> 50.asJson), bo)
    } yield {
      val onJ2 = operations.edges(mine).filter(_.hcursor.downField("job").get[String]("id") == Right(ids("J2")))
      assertEquals(onJ2.size, 1)
      val succeeded = List(outcomes._1, outcomes._2).collect { case StoryOutcome.Succeeded(value) =>
        operations.idOf(value)
      }
      assertEquals(succeeded, List(operations.idOf(onJ2.head)))
      ids.put("BO_J2", succeeded.head)
    }
  }

  test("HEA-04.4 Cy cannot apply to the closed J3") {
    operations.submit(candidate(3), ids("J3")).flatMap { outcome =>
      operations
        .read(StoryDocuments.myApplications, "myApplications", Json.obj("first" -> 50.asJson), candidate(3))
        .map { mine =>
          assertEquals(refusalCode(outcome), "JOB_MUST_BE_OPEN")
          assert(!operations.edges(mine).exists(_.hcursor.downField("job").get[String]("id") == Right(ids("J3"))))
        }
    }
  }

  test("HEA-05.1 Ana sees her two applications; Bo's application is not visible by list or by direct ID") {
    val ana = candidate(1)
    val unknownApplication = UUID.randomUUID().toString
    for {
      anaJ2 <- operations.submitOk(ana, ids("J2"))
      mine <- operations.read(StoryDocuments.myApplications, "myApplications", Json.obj("first" -> 50.asJson), ana)
      boDirect <- operations.query(
        StoryDocuments.applicationHistory,
        "applicationHistory",
        Json.obj("applicationId" -> application(2).asJson, "first" -> 10.asJson),
        Some(ana)
      )
      unknownDirect <- operations.query(
        StoryDocuments.applicationHistory,
        "applicationHistory",
        Json.obj("applicationId" -> unknownApplication.asJson, "first" -> 10.asJson),
        Some(ana)
      )
    } yield {
      ids.put("ANA_J2", anaJ2)
      assertEquals(nodeIds(mine).toSet, Set(application(1), anaJ2))
      assertEquals(nodeIds(mine).size, 2)
      assert(!nodeIds(mine).contains(application(2)))
      // Bo's application is hidden from Ana exactly as an unknown id is: same code, message and shape.
      assertEquals(boDirect, unknownDirect)
      assertEquals(refusalCode(boDirect), "NOT_FOUND")
    }
  }

  test("HEA-08.1 R1 lists the twelve J1 applications in pages; R2 and a candidate are refused") {
    def pages(after: Option[String], collected: Vector[Vector[String]]): IO[Vector[Vector[String]]] =
      operations
        .read(
          StoryDocuments.jobApplications,
          "jobApplications",
          Json.obj("jobId" -> ids("J1").asJson, "first" -> 5.asJson, "after" -> after.asJson),
          rita
        )
        .flatMap { page =>
          val info = page.hcursor.downField("pageInfo")
          val updated = collected :+ nodeIds(page)
          if (info.get[Boolean]("hasNextPage") == Right(true)) pages(info.get[String]("endCursor").toOption, updated)
          else IO.pure(updated)
        }
    for {
      paged <- pages(None, Vector.empty)
      byRavi <- operations.query(
        StoryDocuments.jobApplications,
        "jobApplications",
        Json.obj("jobId" -> ids("J1").asJson, "first" -> 5.asJson),
        Some(ravi)
      )
      byAna <- operations.query(
        StoryDocuments.jobApplications,
        "jobApplications",
        Json.obj("jobId" -> ids("J1").asJson, "first" -> 5.asJson),
        Some(candidate(1))
      )
    } yield {
      assertEquals(paged.map(_.size), Vector(5, 5, 2))
      assertEquals(paged.flatten.toSet, (1 to 12).map(application).toSet)
      assertEquals(paged.flatten.distinct.size, 12)
      // R2 does not own J1, so J1 is a missing job to R2; a candidate has no right to list applications at all.
      assertEquals(refusalCode(byRavi), "NOT_FOUND")
      assertEquals(refusalCode(byAna), "FORBIDDEN")
    }
  }

  private def statusOf(applicationId: String): IO[String] =
    applicationStatuses(ids("J1")).map(_.getOrElse(applicationId, fail(s"$applicationId is not listed under J1")))

  private def mustSucceed(outcome: StoryOutcome, expectedStatus: String): Unit = outcome match {
    case StoryOutcome.Succeeded(value) => assertEquals(asString(value, "status"), expectedStatus)
    case other                         => fail(s"expected $expectedStatus but was $other")
  }

  test("HEA-09.1 R1 moves C01 through Accepted, Interview and Hired; each step appends history with the status") {
    val steps = List(
      "acceptApplication" -> "ACCEPTED",
      "moveApplicationToInterview" -> "INTERVIEW",
      "hireApplication" -> "HIRED"
    )
    steps.foldLeft(IO.unit) { case (previous, (mutation, expected)) =>
      previous *> operations.applicationAction(mutation, rita, application(1)).flatMap { outcome =>
        for {
          status <- statusOf(application(1))
          events <- operations.history(candidate(1), application(1))
        } yield {
          mustSucceed(outcome, expected)
          assertEquals(status, expected)
          assertEquals(asString(chained(events).last, "newStatus"), expected)
        }
      }
    } *> operations.history(candidate(1), application(1)).map { events =>
      val ordered = chained(events)
      assertEquals(
        historyChain(ordered),
        Vector(
          (None, "CREATED"),
          (Some("CREATED"), "ACCEPTED"),
          (Some("ACCEPTED"), "INTERVIEW"),
          (Some("INTERVIEW"), "HIRED")
        )
      )
      assertEquals(ordered.drop(1).map(asString(_, "actorRole")).distinct, Vector("RECRUITER"))
      val times = ordered.map(event => Instant.parse(asString(event, "occurredAt")))
      assertEquals(times, times.sorted)
    }
  }

  test("HEA-09.2 rejecting C02 without feedback fails and changes nothing; with feedback it succeeds") {
    val feedback = "Thank you; we are going with a candidate with more Kafka experience."
    for {
      omitted <- operations.applicationAction("rejectApplication", rita, application(2))
      blank <- operations.applicationAction(
        "rejectApplication",
        rita,
        application(2),
        Json.obj("feedback" -> "   ".asJson)
      )
      unchanged <- statusOf(application(2))
      unchangedHistory <- operations.history(candidate(2), application(2))
      rejected <- operations.applicationAction(
        "rejectApplication",
        rita,
        application(2),
        Json.obj("feedback" -> feedback.asJson)
      )
      status <- statusOf(application(2))
      events <- operations.history(candidate(2), application(2))
    } yield {
      assertEquals(refusalCode(omitted), "REJECTION_FEEDBACK_REQUIRED")
      assertEquals(refusalCode(blank), "REJECTION_FEEDBACK_REQUIRED")
      assertEquals(unchanged, "CREATED")
      assertEquals(unchangedHistory.size, 1)
      mustSucceed(rejected, "REJECTED")
      assertEquals(status, "REJECTED")
      assertEquals(historyChain(chained(events)), Vector((None, "CREATED"), (Some("CREATED"), "REJECTED")))
      assertEquals(asString(chained(events).last, "feedback"), feedback)
    }
  }

  test("HEA-09.3 R1 cannot hire C06 who is in Created") {
    for {
      outcome <- operations.applicationAction("hireApplication", rita, application(6))
      status <- statusOf(application(6))
      events <- operations.history(candidate(6), application(6))
    } yield {
      assertEquals(refusalCode(outcome), "INVALID_STATUS_TRANSITION")
      assertEquals(status, "CREATED")
      assertEquals(events.size, 1)
    }
  }

  test("HEA-09.4 C04 declines only with a reason and cannot decline C05's application") {
    val reason = "I accepted another offer."
    for {
      withoutReason <- operations.applicationAction("declineApplication", candidate(4), application(4))
      stillCreated <- statusOf(application(4))
      foreign <- operations.applicationAction(
        "declineApplication",
        candidate(4),
        application(5),
        Json.obj("reason" -> reason.asJson)
      )
      foreignStatus <- statusOf(application(5))
      declined <- operations.applicationAction(
        "declineApplication",
        candidate(4),
        application(4),
        Json.obj("reason" -> reason.asJson)
      )
      events <- operations.history(candidate(4), application(4))
    } yield {
      assertEquals(refusalCode(withoutReason), "DECLINE_REASON_REQUIRED")
      assertEquals(stillCreated, "CREATED")
      assertEquals(refusalCode(foreign), "NOT_FOUND")
      assertEquals(foreignStatus, "CREATED")
      mustSucceed(declined, "DECLINED")
      assertEquals(historyChain(chained(events)), Vector((None, "CREATED"), (Some("CREATED"), "DECLINED")))
      assertEquals(asString(chained(events).last, "reason"), reason)
    }
  }

  test("HEA-09.5 the rest of the lifecycle story ends with the spec's final status counts for J1") {
    for {
      c3a <- operations.applicationAction("acceptApplication", rita, application(3))
      c3b <- operations.applicationAction("moveApplicationToInterview", rita, application(3))
      c3c <- operations.applicationAction(
        "rejectApplication",
        rita,
        application(3),
        Json.obj("feedback" -> "Not a fit after the interview.".asJson)
      )
      c5 <- operations.applicationAction("acceptApplication", rita, application(5))
      detail <- operations.read(StoryDocuments.job, "job", Json.obj("id" -> ids("J1").asJson), rita)
      statuses <- applicationStatuses(ids("J1"))
    } yield {
      mustSucceed(c3a, "ACCEPTED")
      mustSucceed(c3b, "INTERVIEW")
      mustSucceed(c3c, "REJECTED")
      mustSucceed(c5, "ACCEPTED")
      val expected = Map("HIRED" -> 1, "REJECTED" -> 2, "DECLINED" -> 1, "ACCEPTED" -> 1, "CREATED" -> 7)
      assertEquals(statuses.values.toList.groupMapReduce(identity)(_ => 1)(_ + _), expected)
      val counts = detail.hcursor.downField("applicationCounts")
      assertEquals(counts.get[Int]("total"), Right(12))
      val byStatus = counts
        .downField("byStatus")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .map(entry => asString(entry, "status") -> entry.hcursor.get[Int]("count").fold(e => fail(e.message), identity))
        .filter(_._2 > 0)
        .toMap
      assertEquals(byStatus, expected)
    }
  }

  test("HEA-09.6 declined is terminal: the owning recruiter cannot decline C04's application again") {
    for {
      withReason <- operations.applicationAction(
        "declineApplication",
        rita,
        application(4),
        Json.obj("reason" -> "The candidate withdrew.".asJson)
      )
      status <- statusOf(application(4))
      events <- operations.history(candidate(4), application(4))
    } yield {
      assertEquals(refusalCode(withReason), "INVALID_STATUS_TRANSITION")
      assertEquals(status, "DECLINED")
      assertEquals(historyChain(chained(events)), Vector((None, "CREATED"), (Some("CREATED"), "DECLINED")))
    }
  }

  private val expectedEvents = Map(
    "APPLICATION_CREATED" -> 14, // 12 on J1, Ana on J2, Bo on J2; the duplicate and closed-job attempts commit nothing
    "APPLICATION_STATUS_CHANGED" -> 9, // C01 3, C02 1, C03 3, C04 1, C05 1
    "CANDIDATE_HIRED" -> 1, // C01
    "JOB_VIEWED" -> 1, // the single recorded view; its idempotent replay adds none
    "SEARCH_RESULT_CLICKED" -> 1, // the single recorded click
    "JOB_CREATED" -> 4, // J1 to J4 when published; the Draft J5 announces nothing
    "JOB_UPDATED" -> 1, // J3 updated while open; the refused publish adds none
    "JOB_CLOSED" -> 1 // J3 closed
  )

  private def recordClick(ana: StoryActor): IO[StoryOutcome] = {
    val input = Json.obj(
      "eventId" -> UUID.randomUUID().toString.asJson,
      "searchId" -> ids("SEARCH").asJson,
      "resultId" -> ids("J1").asJson
    )
    // The search session is recorded best-effort and asynchronously; a pending session is retried for a bounded time.
    def attempt(remaining: Int): IO[StoryOutcome] =
      operations.mutate("recordSearchResultClick", input, ana).flatMap {
        case StoryOutcome.Refused(code, _, true) if code == "SEARCH_SESSION_PENDING" && remaining > 0 =>
          IO.sleep(500.millis) *> attempt(remaining - 1)
        case other => IO.pure(other)
      }
    attempt(40)
  }

  private def outboxPending: IO[Long] =
    stack.database.getCollection(outboxCollection).flatMap(_.count(Filters.ne("state", "Published")))

  private def outboxTotal: IO[Long] = stack.database.getCollection(outboxCollection).flatMap(_.count)

  private def untilPublished: IO[Unit] =
    (outboxPending, outboxTotal).tupled.flatMap { case (pending, total) =>
      if (pending == 0L && total > 0L) IO.unit else IO.sleep(250.millis) *> untilPublished
    }

  test("HEA-11.1 after the story the published events per action equal the committed actions") {
    for {
      click <- recordClick(candidate(1))
      _ <- untilPublished.timeout(90.seconds)
      published <- OperationalEventTopic.readAll(stack.running.namespace).timeout(60.seconds)
      total <- outboxTotal
    } yield {
      assert(click.isInstanceOf[StoryOutcome.Succeeded], clues(click))
      val byType = published.map(event => asString(event, "eventType")).groupMapReduce(identity)(_ => 1)(_ + _)
      expectedEvents.foreach { case (eventType, count) =>
        assertEquals(byType.getOrElse(eventType, 0), count, clues(eventType, byType))
      }
      val eventIds = published.map(asString(_, "eventId"))
      assertEquals(eventIds.distinct.size, eventIds.size, "an event was published twice")
      assertEquals(published.size.toLong, total, clues(byType))
      val unexpected = byType.removedAll(expectedEvents.keys)
      println(s"HEA-11.1 published event counts: ${byType.toList.sortBy(_._1).mkString(", ")}")
      println(s"HEA-11.1 types outside the committed-action list: $unexpected")
    }
  }

  test("HEA-11.3 the story's published events are exported for the analytics step") {
    AnalyticsChain.require()
    for {
      _ <- untilPublished.timeout(90.seconds)
      exported <- HiringStoryEventExport.write(stack.running.namespace)
    } yield {
      println(s"HEA-11.3 exported ${exported._2} events to the analytics hand-off file")
      // 14 + 9 + 1 + 1 + 1 + 4 + 1 + 1 published action events plus 12 SEARCH_PERFORMED (see HEA-11.1).
      assertEquals(exported._2, 44)
      assert(java.nio.file.Files.isRegularFile(exported._1))
    }
  }
}
