package com.example.graphQL.cats.repository.mongo

import io.circe.{Json, parser}

/** Pure helpers that turn Atlas Local runner observations into criterion-tagged evidence records. They hold no I/O. The
  * records are correctness evidence from a `local-container` deployment: they never claim Atlas figures, human
  * relevance, or that a setting was adopted.
  */
private[mongo] object AtlasLocalCriterionEvidence {

  /** Decision 4 of the vector-retrieval spec: Recall@K of ANN against exact search. */
  val RecallFloor: Double = 0.95

  /** Decision 4: ANN p95 may exceed the baseline p95 by at most 10 percent. */
  val LatencyTolerance: Double = 1.10

  enum SweepOutcome(val text: String) {
    case RetainCurrentSettings extends SweepOutcome("retain current settings")
    case CandidateForCloudConfirmation extends SweepOutcome("candidate for cloud confirmation")
  }

  final case class BucketFigures(
      bucket: String,
      queries: Int,
      recall: Option[Double],
      annP50: Option[Double],
      annP95: Option[Double],
      annP99: Option[Double],
      errors: Int
  )

  /** Every input that must be held fixed between a baseline and a sweep point, except the swept ones. */
  final case class Settings(
      numCandidates: Int,
      quantization: String,
      branchLimit: Int,
      vectorStorage: String,
      storedSource: Boolean,
      pageSize: Int,
      documents: Int,
      queries: Int,
      dimensions: Int,
      seed: Long
  ) {

    /** Names of the fixed inputs on which the two settings differ (`numCandidates` and `quantization` are swept). */
    def fixedDifferences(other: Settings): List[String] = List(
      "branchLimit" -> (branchLimit != other.branchLimit),
      "vectorStorage" -> (vectorStorage != other.vectorStorage),
      "storedSource" -> (storedSource != other.storedSource),
      "pageSize" -> (pageSize != other.pageSize),
      "documents" -> (documents != other.documents),
      "queries" -> (queries != other.queries),
      "dimensions" -> (dimensions != other.dimensions),
      "seed" -> (seed != other.seed)
    ).collect { case (name, true) => name }

    def sweptDifferences(other: Settings): List[String] = List(
      "numCandidates" -> (numCandidates != other.numCandidates),
      "quantization" -> (quantization != other.quantization)
    ).collect { case (name, true) => name }
  }

  final case class RunFigures(
      settings: Settings,
      concurrency: Int,
      temperature: String,
      indexBytes: Option[Long],
      buckets: List[BucketFigures]
  )

  /** One paired query: ANN and exact top ids, ANN latency, and whether either side failed. */
  final case class Sample(ann: List[String], exact: List[String], annLatencyMillis: Double, failed: Boolean)

  /** Nearest-rank percentile; absent for an empty sample. */
  def percentile(values: List[Double], fraction: Double): Option[Double] =
    Option.when(values.nonEmpty) {
      val sorted = values.sorted
      val rank = math.ceil(fraction * sorted.size).toInt.max(1).min(sorted.size)
      sorted(rank - 1)
    }

  /** Share of the exact top ids that ANN also returned; absent when the exact result is empty. */
  def recall(ann: List[String], exact: List[String]): Option[Double] =
    Option.when(exact.nonEmpty)(exact.count(ann.toSet).toDouble / exact.size)

  def bucketFigures(bucket: String, samples: List[Sample]): BucketFigures = {
    val succeeded = samples.filterNot(_.failed)
    val recalls = succeeded.flatMap(sample => recall(sample.ann, sample.exact))
    val latencies = succeeded.map(_.annLatencyMillis)
    BucketFigures(
      bucket,
      samples.size,
      Option.when(recalls.nonEmpty)(recalls.sum / recalls.size),
      percentile(latencies, 0.50),
      percentile(latencies, 0.95),
      percentile(latencies, 0.99),
      samples.count(_.failed)
    )
  }

  /** The bucket a synthetic query belongs to, matching the runner's filter groups (`q-N`, N mod 3). */
  def bucketName(queryNumber: Int): String = queryNumber % 3 match {
    case 0 => "Broad"
    case 1 => "Selective"
    case _ => "Empty"
  }

  /** Decision 4 for one run against its baseline. A candidate for cloud confirmation needs the same concurrency and
    * temperature, different settings, zero errors, Recall >= 0.95 and ANN p95 <= baseline p95 + 10% in every bucket,
    * and an improvement in at least one measured dimension (a bucket p95 or the index bytes). Anything else, including
    * a missing baseline, retains the current settings. Nothing here adopts a change.
    */
  def decide(baseline: Option[RunFigures], candidate: RunFigures): (SweepOutcome, String) = {
    val retain = SweepOutcome.RetainCurrentSettings
    baseline match {
      case None => (retain, "no baseline record supplied; this run cannot be compared")
      case Some(base) if base.concurrency != candidate.concurrency || base.temperature != candidate.temperature =>
        (retain, "baseline concurrency or temperature differs; not comparable")
      case Some(base) if base.settings.fixedDifferences(candidate.settings).nonEmpty =>
        (
          retain,
          s"baseline differs in fixed inputs: ${base.settings.fixedDifferences(candidate.settings).mkString(", ")}"
        )
      case Some(base) if base.settings.sweptDifferences(candidate.settings).isEmpty =>
        (retain, "settings equal the baseline")
      case Some(base) if base.settings.sweptDifferences(candidate.settings).size > 1 =>
        (retain, "more than one swept setting differs from the baseline")
      case Some(base) =>
        val pairs = candidate.buckets.map(bucket => bucket -> base.buckets.find(_.bucket == bucket.bucket))
        val failures = pairs.flatMap { case (bucket, reference) =>
          val recallOk = bucket.recall.forall(_ >= RecallFloor)
          val errorsOk = bucket.errors == 0
          val latencyOk = (bucket.annP95, reference.flatMap(_.annP95)) match {
            case (Some(value), Some(limit)) => value <= limit * LatencyTolerance
            case _                          => false
          }
          List(
            Option.when(!recallOk)(s"${bucket.bucket}: Recall below $RecallFloor"),
            Option.when(!errorsOk)(s"${bucket.bucket}: errors observed"),
            Option.when(!latencyOk)(s"${bucket.bucket}: ANN p95 above baseline + 10% or unmeasured")
          ).flatten
        }
        val faster = pairs.exists { case (bucket, reference) =>
          (bucket.annP95, reference.flatMap(_.annP95)).both.exists { case (value, limit) => value < limit }
        }
        val smaller = (candidate.indexBytes, base.indexBytes).both.exists { case (value, limit) => value < limit }
        if (failures.nonEmpty) (retain, failures.mkString("; "))
        else if (!faster && !smaller) (retain, "no measured dimension improved over the baseline")
        else (SweepOutcome.CandidateForCloudConfirmation, "all buckets meet decision 4 and a dimension improved")
    }
  }

  enum DiscoveryOutcome(val text: String) {
    case RetainCurrent extends DiscoveryOutcome("retain current")
    case CandidateForCloudConfirmation extends DiscoveryOutcome("candidate for cloud confirmation")
  }

  /** What one geographic pipeline did over the fixed workload: result count, examined work and latency. */
  final case class PlanFigures(
      returned: Long,
      docsExamined: Long,
      keysExamined: Long,
      p95Millis: Double,
      errors: Int
  )

  /** HDQ-11.L: a different pipeline is only a candidate for cloud confirmation when it returns the same job ids for
    * every query, has no errors, examines no more documents, is no slower at p95, and is better in at least one of
    * examined documents or p95 (by 10%). Anything else retains the current pipeline. Nothing is adopted here.
    */
  def decideDiscovery(
      current: PlanFigures,
      candidate: PlanFigures,
      identicalResults: Boolean
  ): (DiscoveryOutcome, String) = {
    val retain = DiscoveryOutcome.RetainCurrent
    if (!identicalResults) (retain, "result ids differ from the current pipeline")
    else if (candidate.errors > 0 || current.errors > 0) (retain, "errors observed")
    else if (candidate.docsExamined > current.docsExamined) (retain, "candidate examines more documents")
    else if (candidate.p95Millis > current.p95Millis) (retain, "candidate p95 is slower")
    else if (candidate.docsExamined < current.docsExamined)
      (DiscoveryOutcome.CandidateForCloudConfirmation, "same results, no regression, and fewer documents examined")
    else if (candidate.p95Millis <= current.p95Millis * 0.9)
      (
        DiscoveryOutcome.CandidateForCloudConfirmation,
        "latency-only, repeat: same results and no regression, improvement only in p95"
      )
    else (retain, "no measured improvement")
  }

  /** The spec requires every bucket and concurrency: one retained run retains the setting. */
  def combine(outcomes: List[SweepOutcome]): SweepOutcome =
    if (outcomes.nonEmpty && outcomes.forall(_ == SweepOutcome.CandidateForCloudConfirmation))
      SweepOutcome.CandidateForCloudConfirmation
    else SweepOutcome.RetainCurrentSettings

  extension [A, B](pair: (Option[A], Option[B]))
    private def both: Option[(A, B)] = for { a <- pair._1; b <- pair._2 } yield (a, b)

  private def optional(value: Option[Double]): Json = value.fold(Json.Null)(Json.fromDoubleOrNull)

  def figuresJson(figures: RunFigures): Json = Json.obj(
    "settings" -> Json.obj(
      "numCandidates" -> Json.fromInt(figures.settings.numCandidates),
      "quantization" -> Json.fromString(figures.settings.quantization),
      "branchLimit" -> Json.fromInt(figures.settings.branchLimit),
      "vectorStorage" -> Json.fromString(figures.settings.vectorStorage),
      "storedSource" -> Json.fromBoolean(figures.settings.storedSource),
      "pageSize" -> Json.fromInt(figures.settings.pageSize),
      "documents" -> Json.fromInt(figures.settings.documents),
      "queries" -> Json.fromInt(figures.settings.queries),
      "dimensions" -> Json.fromInt(figures.settings.dimensions),
      "seed" -> Json.fromLong(figures.settings.seed)
    ),
    "concurrency" -> Json.fromInt(figures.concurrency),
    "temperature" -> Json.fromString(figures.temperature),
    "indexBytes" -> figures.indexBytes.fold(Json.Null)(Json.fromLong),
    "buckets" -> Json.fromValues(figures.buckets.map { bucket =>
      Json.obj(
        "bucket" -> Json.fromString(bucket.bucket),
        "queries" -> Json.fromInt(bucket.queries),
        "recallAtKAgainstExact" -> optional(bucket.recall),
        "annP50Millis" -> optional(bucket.annP50),
        "annP95Millis" -> optional(bucket.annP95),
        "annP99Millis" -> optional(bucket.annP99),
        "errors" -> Json.fromInt(bucket.errors)
      )
    })
  )

  /** Reads the `figures` object of an earlier record written by [[figuresJson]]. */
  def parseFigures(text: String): Either[String, RunFigures] =
    parser.parse(text).left.map(_ => "Baseline record is not valid JSON").flatMap { root =>
      val cursor = root.hcursor.downField("figures")
      def bucket(json: Json): Either[String, BucketFigures] = {
        val c = json.hcursor
        for {
          name <- c.get[String]("bucket").left.map(_ => "Baseline bucket name missing")
          queries <- c.get[Int]("queries").left.map(_ => "Baseline bucket queries missing")
          errors <- c.get[Int]("errors").left.map(_ => "Baseline bucket errors missing")
        } yield BucketFigures(
          name,
          queries,
          c.get[Option[Double]]("recallAtKAgainstExact").toOption.flatten,
          c.get[Option[Double]]("annP50Millis").toOption.flatten,
          c.get[Option[Double]]("annP95Millis").toOption.flatten,
          c.get[Option[Double]]("annP99Millis").toOption.flatten,
          errors
        )
      }
      for {
        settings <- {
          val c = cursor.downField("settings")
          (for {
            numCandidates <- c.get[Int]("numCandidates")
            quantization <- c.get[String]("quantization")
            branchLimit <- c.get[Int]("branchLimit")
            vectorStorage <- c.get[String]("vectorStorage")
            storedSource <- c.get[Boolean]("storedSource")
            pageSize <- c.get[Int]("pageSize")
            documents <- c.get[Int]("documents")
            queries <- c.get[Int]("queries")
            dimensions <- c.get[Int]("dimensions")
            seed <- c.get[Long]("seed")
          } yield Settings(
            numCandidates,
            quantization,
            branchLimit,
            vectorStorage,
            storedSource,
            pageSize,
            documents,
            queries,
            dimensions,
            seed
          )).left.map(_ => "Baseline settings missing or invalid")
        }
        concurrency <- cursor.get[Int]("concurrency").left.map(_ => "Baseline concurrency missing")
        temperature <- cursor.get[String]("temperature").left.map(_ => "Baseline temperature missing")
        buckets <- cursor.get[List[Json]]("buckets").left.map(_ => "Baseline buckets missing")
        parsed <- buckets.traverseEither(bucket)
      } yield RunFigures(
        settings,
        concurrency,
        temperature,
        cursor.get[Option[Long]]("indexBytes").toOption.flatten,
        parsed
      )
    }

  extension [A](values: List[A])
    private def traverseEither[B](f: A => Either[String, B]): Either[String, List[B]] =
      values.foldRight(Right(Nil): Either[String, List[B]])((value, rest) =>
        for { b <- f(value); tail <- rest } yield b :: tail
      )

  /** The sweep record: criteria it supports, the environment label, figures, and the decision with its reason. */
  def searchEvaluationRecord(
      deployment: String,
      atlasVersion: String,
      sourceRevision: String,
      reportFile: String,
      durationMillis: Long,
      figures: RunFigures,
      baselineFile: Option[String],
      decision: (SweepOutcome, String)
  ): Json = {
    val queries = figures.buckets.map(_.queries).sum
    Json.obj(
      "criteria" -> Json.arr(List("MVR-07.L", "HSE-06.L", "MVR-08.L").map(Json.fromString)*),
      "environment" -> Json.fromString(deployment),
      "atlasVersion" -> Json.fromString(atlasVersion),
      "sourceRevision" -> Json.fromString(sourceRevision),
      "reportFile" -> Json.fromString(reportFile),
      "queries" -> Json.fromInt(queries),
      "throughputQueriesPerSecond" ->
        Json.fromDoubleOrNull(if (durationMillis > 0L) queries * 1000.0 / durationMillis else 0.0),
      "figures" -> figuresJson(figures),
      "unavailableTelemetry" -> Json.arr(
        List("index build time", "cpu", "memory", "atlas query metrics", "write p95").map(Json.fromString)*
      ),
      "mvr08" -> Json.obj(
        "baselineRecord" -> baselineFile.fold(Json.Null)(Json.fromString),
        "outcome" -> Json.fromString(decision._1.text),
        "reason" -> Json.fromString(decision._2),
        "scope" -> Json.fromString(
          "Run-level verdict only: a setting is a candidate for cloud confirmation when the records for concurrency 1 and 8, cold and warm, all say so. No change is adopted; only MVR-08.C can adopt."
        )
      ),
      "scope" -> Json.fromString(
        "local-container correctness evidence: not Atlas figures, no human-relevance claim, provider requests zero."
      )
    )
  }

  /** The curated-capture record for SEB-02.L and SEB-04.L, naming the separated result groups and fixture tags. */
  def curatedRecord(
      deployment: String,
      atlasVersion: Option[String],
      sourceRevision: String,
      concurrency: Int,
      reportFiles: List[String],
      fixtureTags: Map[String, Int]
  ): Json = Json.obj(
    "criteria" -> Json.arr(List("SEB-02.L", "SEB-04.L").map(Json.fromString)*),
    "environment" -> Json.fromString(deployment),
    "atlasVersion" -> atlasVersion.fold(Json.Null)(Json.fromString),
    "sourceRevision" -> Json.fromString(sourceRevision),
    "concurrency" -> Json.fromInt(concurrency),
    "captures" -> Json.arr(reportFiles.map(Json.fromString)*),
    "seb02" -> Json.obj(
      "separatedResultGroups" -> Json.arr(
        List("fidelity", "relevance input", "use case", "filter group").map(Json.fromString)*
      ),
      "vectors" -> Json.fromString("deterministic synthetic, nonsemantic"),
      "labels" -> Json.fromString("provisional fabricated")
    ),
    "seb04" -> Json.obj(
      "fixtureTags" -> Json.obj(fixtureTags.toList.sortBy(_._1).map { case (tag, count) =>
        tag -> Json.fromInt(count)
      }*),
      "authorizationBeforeRetrieval" -> Json.fromString(
        "asserted by AtlasHiringSearchBoundsIntegrationSpec tests tagged SEB-04.L, not by this runner"
      )
    ),
    "scope" -> Json.fromString(
      "local-container correctness evidence: semantic quality and privacy acceptance remain unobserved."
    )
  )
}
