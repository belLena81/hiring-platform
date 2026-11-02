package com.example.hiring.testing

/** The fictional jobs and candidates of the search part of the hiring story
  * (docs/specs/hiring-end-to-end-acceptance.md, "Tier B expected outcomes"). The same texts feed the Tier B suite and
  * the unit test that fixes the stub's ranking oracle, so the expected order is derived from these words and not from a
  * run.
  */
object HiringSearchStoryData {
  final case class JobSeed(
      key: String,
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      city: String
  ) {

    /** The searchable text, in the order of `SearchableText.job` (title, description, requirements, skills). */
    def searchableText: String =
      (List(title, description, "Requirements:") ++ requirements ++ List("Skills:", skills.sorted.mkString(", ")))
        .mkString("\n")
  }

  final case class CandidateSeed(
      key: String,
      name: String,
      skills: List[String],
      experienceSummary: String,
      city: String,
      recruiterSearchOptIn: Boolean
  ) {

    /** The searchable text, in the order of `SearchableText.candidate` (summary, skills). */
    def searchableText: String = List(experienceSummary, "Skills:", skills.sorted.mkString(", ")).mkString("\n")
  }

  /** J3 and J5 deliberately contain the query words "JVM" and "backend": if visibility leaked, they would rank. */
  val j1: JobSeed = JobSeed(
    "J1",
    "Scala Backend",
    "Build backend services on the JVM with Scala and Kafka",
    List("Functional programming experience"),
    List("Scala", "Kafka"),
    "Berlin"
  )
  val j2: JobSeed = JobSeed(
    "J2",
    "Kotlin Services",
    "Maintain payment services written in Kotlin",
    List("On-call rota participation"),
    List("Kotlin", "Spring"),
    "Berlin"
  )
  val j3: JobSeed = JobSeed(
    "J3",
    "Java Developer",
    "Maintain legacy JVM backend applications",
    List("Application server experience"),
    List("Java"),
    "Munich"
  )
  val j4: JobSeed = JobSeed(
    "J4",
    "Data Engineer",
    "Build warehouse pipelines and reporting models",
    List("Batch processing experience"),
    List("Spark", "SQL"),
    "Berlin"
  )
  val j5: JobSeed = JobSeed(
    "J5",
    "Backend Prototype",
    "Experimental JVM backend prototype kept private",
    List("Prototype experience"),
    List("Rust"),
    "Berlin"
  )

  val ana: CandidateSeed =
    CandidateSeed("C01", "Ana Rivera", List("Scala", "Kafka"), "Five years of backend work", "Berlin", true)
  val bo: CandidateSeed =
    CandidateSeed("C02", "Bo Lindqvist", List("Kotlin"), "Seven years of Android development", "Munich", true)
  val cy: CandidateSeed =
    CandidateSeed("C03", "Cy Okafor", List("Java"), "Six years of enterprise integration", "Karlsruhe", true)

  /** Opted out of recruiter search, with a strong Scala profile and a residence that must never be revealed. */
  val dee: CandidateSeed =
    CandidateSeed("C04", "Dee Haddad", List("Scala"), "Four years of Scala services", "Hamburg", false)

  val candidates: List[CandidateSeed] = List(ana, bo, cy, dee)
}
