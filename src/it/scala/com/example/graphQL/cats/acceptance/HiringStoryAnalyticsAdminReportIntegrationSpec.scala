package com.example.graphQL.cats.acceptance

/** The 12-candidate story's report read through the real Admin `analyticsReport` (access control; the report itself is
  * empty at this scale by the K=10 rule and is checked on the hired cohort).
  */
class HiringStoryAnalyticsAdminReportIntegrationSpec
    extends StoryAdminReportSuite("story-events.jsonl", "report-documents.jsonl") {
  test("HEA-12.2 a candidate and a recruiter are forbidden to read the report; the Admin is not") {
    requireChain()
    for {
      admin <- adminActor
      recruiter <- operations.signUpRecruiter("Rhea Analytics", "Fabrikam Hiring")
      candidate <- operations.signUpCandidate("Cal Analytics", List("Scala"))
      _ <- storePublishedReport
      asCandidate <- operations.query(query, "analyticsReport", period, Some(candidate))
      asRecruiter <- operations.query(query, "analyticsReport", period, Some(recruiter))
      anonymous <- operations.query(query, "analyticsReport", period, None)
      asAdmin <- operations.query(query, "analyticsReport", period, Some(admin))
    } yield {
      def code(outcome: StoryOutcome): String = outcome match {
        case StoryOutcome.Refused(value, _, _) => value
        case other                             => fail(s"expected a refusal but was $other")
      }
      assertEquals(code(asCandidate), "UNAUTHORIZED")
      assertEquals(code(asRecruiter), "UNAUTHORIZED")
      assert(Set("UNAUTHENTICATED", "UNAUTHORIZED", "HTTP_401").contains(code(anonymous)), clues(anonymous))
      assert(asAdmin.isInstanceOf[StoryOutcome.Succeeded], clues(asAdmin))
    }
  }
}
