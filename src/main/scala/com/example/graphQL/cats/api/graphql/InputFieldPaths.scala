package com.example.graphQL.cats.api.graphql

/** Maps the field name a domain validation rule reports to the GraphQL input path of the operation that was called, so
  * a client can attach `ValidationError.field` to the input it sent. A domain field with no input counterpart maps to
  * `None`.
  */
private[graphql] object InputFieldPaths {
  type Path = String => Option[String]

  /** For operations whose input has no domain-validated field. */
  val none: Path = _ => None

  private def same(names: String*): Map[String, String] = names.map(name => name -> name).toMap

  private val candidateProfile: Map[String, String] = Map(
    "skills" -> "profile.candidate.skills",
    "experienceSummary" -> "profile.candidate.experienceSummary",
    "resumeRef" -> "profile.candidate.resumeRef",
    "currentResidence.country" -> "profile.candidate.residence.country",
    "currentResidence.city" -> "profile.candidate.residence.city"
  )
  private val recruiterProfile: Map[String, String] = Map(
    "organizationName" -> "profile.recruiter.organizationName",
    "jobTitle" -> "profile.recruiter.jobTitle"
  )
  private val jobContent: List[String] =
    List("title", "description", "requirements", "skills", "country", "city", "coordinates")

  private def from(mapping: Map[String, String]): Path = mapping.get

  val login: Path = from(same("name", "password"))
  val signUp: Path = from(same("name", "password", "profile") ++ candidateProfile ++ recruiterProfile)
  val updateMyProfile: Path = from(same("profile") ++ candidateProfile ++ recruiterProfile)
  val createJob: Path = from(same(jobContent*))
  val updateJob: Path = from(jobContent.map(name => name -> s"job.$name").toMap)
  private val jobFilterPaths: Map[String, String] =
    Map("city" -> "filter.city", "skills" -> "filter.skills", "createdAfter" -> "filter.createdAfter")
  private val pagePaths: Map[String, String] = Map("first" -> "first", "pageSize" -> "first")
  val semanticJobSearch: Path = from(same("query") ++ jobFilterPaths ++ pagePaths)
  val recommendedJobs: Path = from(pagePaths)
  val candidateMatches: Path = from(
    same("query") ++ pagePaths ++ Map(
      "availabilityStatus" -> "filter.availabilityStatus",
      "requiredSkills" -> "filter.requiredSkills",
      "residence" -> "filter.country"
    )
  )
  val jobDiscoveryFacets: Path = from(same("query") ++ jobFilterPaths ++ Map("radius" -> "radiusKm"))
  val rejectApplication: Path = from(same("feedback"))
  val declineApplication: Path = from(same("reason"))
}
