package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.{AnalyticsSubjectPrivacy, OperationalEventTransforms}

import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  GetJsonObject,
  JsonObjectKeys,
  ScalaUDF
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{col, current_timestamp, encode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/** The parse, validate and tokenize work of one batch is computed once and read by every consumer. */
class OperationalEventParsedFrameSpec extends FunSuite {
  private lazy val spark = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("OperationalEventParsedFrameSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  override def afterAll(): Unit = if (!spark.sparkContext.isStopped) spark.stop()

  private val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(11))
  private def id(label: String): String = UUID.nameUUIDFromBytes(label.getBytes("UTF-8")).toString
  private def token(subject: String): String = AnalyticsTestSubjectPseudonymizer.tokenValue(keys, subject)
  private val At = "2026-10-02T10:00:00Z"

  private def statusChanged(event: String, actor: String, candidate: String): String = {
    val app = id(s"app-$event")
    s"""{"eventId":"${id(
        event
      )}","eventType":"APPLICATION_STATUS_CHANGED","occurredAt":"$At","aggregateType":"Application","aggregateId":"$app","actorId":"${id(
        actor
      )}","payload":{"applicationId":"$app","candidateId":"${id(candidate)}","jobId":"${id(
        "job"
      )}","previousStatus":"Created","newStatus":"Accepted"}}"""
  }

  private def search(event: String, actor: String, kind: String, results: Seq[String]): String = {
    val searchId = id(s"search-$event")
    val body = results.zipWithIndex
      .map { case (result, index) => s"""{"resultId":"${id(result)}","rank":${index + 1},"score":0.5}""" }
      .mkString(",")
    s"""{"eventId":"${id(
        event
      )}","eventType":"SEARCH_PERFORMED","occurredAt":"$At","aggregateType":"Search","aggregateId":"$searchId","actorId":"${id(
        actor
      )}","payload":{"searchId":"$searchId","searchKind":"$kind","results":[$body]}}"""
  }

  private def jobCreated(event: String, actor: String): String = {
    val job = id(s"job-$event")
    s"""{"eventId":"${id(
        event
      )}","eventType":"JOB_CREATED","occurredAt":"$At","aggregateType":"Job","aggregateId":"$job","actorId":"${id(
        actor
      )}","payload":{"job":{"jobId":"$job","status":"Open","skills":["Scala"]}}}"""
  }

  private def parsed(values: Seq[String]): DataFrame =
    OperationalEventTransforms.parseKafkaRecords(rawRecords(values))

  private def rawRecords(values: Seq[String]): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType),
        StructField("value", StringType)
      )
    )
    val raw = spark
      .createDataFrame(
        values.zipWithIndex.map { case (value, index) => Row("hiring.events", 0, index.toLong, value) }.asJava,
        schema
      )
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", current_timestamp())
    raw
  }

  private def markers(tokens: Seq[String]): DataFrame =
    spark.createDataFrame(
      tokens.map(Row(_)).asJava,
      StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    )

  private def expressions(plan: LogicalPlan): Seq[Expression] =
    plan.collect { case node => node.expressions }.flatten.flatMap(_.collect { case e => e })

  private def isOn(expression: Expression, column: String): Boolean =
    expression.children.headOption.exists {
      case attribute: AttributeReference => attribute.name == column
      case _                             => false
    }

  private def sha256(value: String): String =
    MessageDigest.getInstance("SHA-256").digest(value.getBytes("UTF-8")).map("%02x".format(_)).mkString

  private val mixed: Seq[String] = Vector(
    statusChanged("status", "recruiter", "candidate"),
    search("candidate-search", "searcher", "candidateMatches", Seq("found-a", "found-b")),
    search("job-search", "searcher", "jobs", Seq("job-a")),
    jobCreated("job-event", "recruiter"),
    "not-json",
    statusChanged("missing-field", "recruiter", "candidate").replace(""""jobId":"""", """"unknown":""""),
    statusChanged("status", "recruiter", "candidate").replace(""""newStatus":"Accepted"""", """"newStatus":"Bogus"""")
  )

  test(
    "AC-02.1 the parsed frame carries validity, SHA-256 fingerprint and tokens that equal independent expectations"
  ) {
    val analyzed = OperationalEventTransforms.analyzed(parsed(mixed), keys)
    val rows = analyzed
      .select("topic", "partition", "offset", "isValidEvent", "eventFingerprint", "subjectToken", "subjectTokens")
      .collect()
      .sortBy(_.getLong(2))
    assertEquals(rows.map(_.getBoolean(3)).toSeq, Seq(true, true, true, true, false, false, false))
    assertEquals(rows.map(_.getString(4)).toSeq, mixed.map(sha256))
    val status = rows(0)
    assertEquals(status.getString(5), token(id("candidate")))
    assertEquals(status.getSeq[String](6).toSet, Set(token(id("recruiter")), token(id("candidate"))))
    val candidateSearch = rows(1)
    assertEquals(candidateSearch.getString(5), token(id("searcher")))
    assertEquals(
      candidateSearch.getSeq[String](6).toList,
      List(token(id("searcher")), token(id("found-a")), token(id("found-b")))
    )
    val jobSearch = rows(2)
    assertEquals(jobSearch.getSeq[String](6).toList, List(token(id("searcher"))))
    assertEquals(rows(3).getString(5), token(id("recruiter")))
    // A malformed record is still attributed once, exactly as the quarantine path needs: no actor means no token.
    assert(rows(4).isNullAt(5))
    assertEquals(rows(4).getSeq[String](6).toList, Nil)
    assertEquals(rows(5).getString(5), token(id("candidate")))
  }

  test("AC-02.1 consumers of the analyzed frame select the same records as validation evaluated inline") {
    val inline = parsed(mixed)
    val analyzed = OperationalEventTransforms.analyzed(inline, keys)
    def ids(frame: DataFrame): Set[Long] = frame.select("offset").collect().map(_.getLong(0)).toSet
    assertEquals(
      ids(OperationalEventTransforms.validEvents(analyzed)),
      ids(OperationalEventTransforms.validEvents(inline))
    )
    assertEquals(
      ids(OperationalEventTransforms.malformedEvents(analyzed)),
      ids(OperationalEventTransforms.malformedEvents(inline))
    )
    assertEquals(ids(OperationalEventTransforms.validEvents(analyzed)), Set(0L, 1L, 2L, 3L))
    // Analysis is idempotent: a frame that already carries the columns is not recomputed.
    assertEquals(
      OperationalEventTransforms.analyzed(analyzed, keys).queryExecution.analyzed.treeString,
      analyzed.queryExecution.analyzed.treeString
    )
    val silver = OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(analyzed), keys, markers(Nil))
    assertEquals(silver.toOption.get.count(), 4L)
  }

  private def accepted(values: Seq[String]): Seq[Boolean] =
    OperationalEventTransforms
      .analyzed(parsed(values), keys)
      .select("offset", "isValidEvent")
      .collect()
      .sortBy(_.getLong(0))
      .map(_.getBoolean(1))
      .toSeq

  private val statusBase = statusChanged("edge-status", "recruiter", "candidate")
  private val jobBase = jobCreated("edge-job", "recruiter")
  private def viewed(payload: String): String = {
    s"""{"eventId":"${id(
        "edge-view"
      )}","eventType":"JOB_VIEWED","occurredAt":"$At","aggregateType":"Search","aggregateId":"${id(
        "edge-result"
      )}","actorId":"${id("viewer")}","payload":$payload}"""
  }
  private def viewedPayload(searchId: String, searchKind: String, rank: String): String =
    s"""{"searchId":$searchId,"resultId":"${id("edge-result")}","searchKind":$searchKind,"rank":$rank}"""

  test("AC-02.1 duplicate keys and non-object envelopes or payloads are malformed") {
    val duplicateEnvelope = statusBase.dropRight(1) + s""","actorId":"${id("other")}"}"""
    val duplicatePayload =
      statusBase.replace(""""previousStatus":"Created",""", """"previousStatus":"Created","jobId":"x",""")
    val payloadString = statusBase.replaceFirst(""""payload":\{.*\}\}$""", """"payload":"{}"}""")
    val payloadArray = statusBase.replaceFirst(""""payload":\{.*\}\}$""", """"payload":[]}""")
    val payloadNull = statusBase.replaceFirst(""""payload":\{.*\}\}$""", """"payload":null}""")
    val nonObjects = Seq("[]", "\"x\"", "5", "null", "{}")
    assertEquals(
      accepted(
        Seq(statusBase, duplicateEnvelope, duplicatePayload, payloadString, payloadArray, payloadNull) ++ nonObjects
      ),
      Seq(true) ++ Seq.fill(5 + nonObjects.size)(false)
    )
  }

  test("AC-02.1 a job that is a string, array, null or missing a key is malformed") {
    def withJob(job: String) = jobBase.replaceFirst(""""job":\{[^}]*\}""", s""""job":$job""")
    assertEquals(
      accepted(
        Seq(
          jobBase,
          withJob("\"x\""),
          withJob("[]"),
          withJob("null"),
          withJob("""{"jobId":"%s","status":"Open"}""".format(id("job-edge-job"))),
          withJob("""{"jobId":"%s","status":"Open","skills":["Scala"],"x":1}""".format(id("job-edge-job"))),
          withJob("""{"jobId":"%s","status":"Open","skills":["Sc\\u0061la"]}""".format(id("job-edge-job"))),
          withJob("""{"jobId":"%s","status":"Open","skills":["Scala",1]}""".format(id("job-edge-job")))
        )
      ),
      Seq(true, false, false, false, false, false, true, false)
    )
  }

  test("AC-02.1 escaped and unicode-escaped JSON is read like its decoded form") {
    val escapedInstant = statusBase.replace(s""""occurredAt":"$At"""", """"occurredAt":"2026-10-02T10:00:00\u005a"""")
    val escapedKey = statusBase.replace(""""actorId":""", """"actor\u0049d":""")
    val escapedId = statusBase
      .replace(id("edge-status"), "\\u0030" + id("edge-status").drop(1))
      .replace(
        s""""eventId":"\\u0030${id("edge-status").drop(1)}"""",
        s""""eventId":"${id("edge-status")}""""
      )
    assertEquals(accepted(Seq(statusBase, escapedInstant, escapedKey, escapedId)), Seq(true, true, true, true))
  }

  test("AC-02.1 null previousStatus is accepted while an absent or non-string previousStatus is malformed") {
    val nulled = statusBase.replace(""""previousStatus":"Created"""", """"previousStatus":null""")
    val absent = statusBase.replace(""""previousStatus":"Created",""", "")
    val number = statusBase.replace(""""previousStatus":"Created"""", """"previousStatus":5""")
    val objectValue = statusBase.replace(""""previousStatus":"Created"""", """"previousStatus":{}""")
    assertEquals(accepted(Seq(nulled, absent, number, objectValue)), Seq(true, false, false, false))
  }

  test("AC-02.1 null versus absent searchKind, searchId and rank on a viewed result") {
    val search = id("edge-view-search")
    def view(searchId: String, searchKind: String, rank: String) = viewed(viewedPayload(searchId, searchKind, rank))
    val q = (value: String) => "\"" + value + "\""
    assertEquals(
      accepted(
        Seq(
          view("null", "null", "null"),
          view(q(search), "null", "3"),
          view("null", "null", "3"),
          view(q(search), "null", "null"),
          view(q(search), q("jobs"), "3"),
          view(q(search), "null", q("3")),
          view(q(search), "null", "3.5"),
          viewed(viewedPayload(q(search), "null", "3").replace(""","searchKind":null""", ""))
        )
      ),
      Seq(true, true, false, false, false, false, false, false)
    )
  }

  test("AC-02.1 search results with extra fields, string numbers or a non-array are malformed") {
    val base = search("edge-results", "searcher", "candidateMatches", Seq("r1"))
    val result = s"""{"resultId":"${id("r1")}","rank":1,"score":0.5}"""
    assert(base.contains(result))
    def with_(replacement: String) = base.replace(result, replacement)
    assertEquals(
      accepted(
        Seq(
          base,
          with_(s"""{"resultId":"${id("r1")}","rank":1,"score":0.5,"extra":1}"""),
          with_(s"""{"resultId":"${id("r1")}","rank":1,"score":"0.5"}"""),
          with_(s"""{"resultId":"${id("r1")}","rank":"1","score":0.5}"""),
          with_(s"""{"resultId":"${id("r1")}","rank":1}"""),
          base.replace(s"[$result]", "{}"),
          base.replace(s"[$result]", "null"),
          with_(s"""{"resultId":"${id("r1")}","resultId":"${id("r1")}","rank":1,"score":0.5}""")
        )
      ),
      Seq(true, false, false, false, false, false, false, false)
    )
  }

  test("AC-02.1 existing isValidEvent and subjectTokens columns are trusted only as the pipeline's own output") {
    val own = OperationalEventTransforms.analyzed(parsed(Seq(statusBase)), keys)
    val forged = own.withColumn("isValidEvent", org.apache.spark.sql.functions.lit(false))
    // Documented contract: analysis keeps an existing column and never recomputes it.
    assertEquals(OperationalEventTransforms.analyzed(forged, keys).select("isValidEvent").head().getBoolean(0), false)
    assertEquals(OperationalEventTransforms.validEvents(forged).count(), 0L)
    assertEquals(OperationalEventTransforms.validEvents(parsed(Seq(statusBase))).count(), 1L)
  }

  private def tokenRows(rows: Seq[Row], schema: StructType): DataFrame = spark.createDataFrame(rows.asJava, schema)
  private val coordinateSchema = StructType(
    Seq(
      StructField("topic", StringType),
      StructField("partition", IntegerType),
      StructField("offset", LongType),
      StructField("eventId", StringType),
      StructField("subjectToken", StringType),
      StructField("subjectTokens", org.apache.spark.sql.types.ArrayType(StringType))
    )
  )

  test("AC-02.1 marker exclusion keeps null and empty token lists and over-excludes duplicate coordinates") {
    val frame = tokenRows(
      Seq(
        Row("t", 0, 1L, "nulls", null, null),
        Row("t", 0, 2L, "empty", null, Seq.empty[String]),
        Row("t", 0, 3L, "kept", "k", Seq("k")),
        Row("t", 0, 4L, "dup-a", "m", Seq("m")),
        Row("t", 0, 4L, "dup-b", "k", Seq("k")),
        Row("t", 0, 5L, "gone", "m", Seq("x", "m"))
      ),
      coordinateSchema
    )
    val kept = AnalyticsSubjectPrivacy
      .excludeActiveDeletionMarkers(frame, markers(Seq("m")))
      .toOption
      .get
      .select("eventId")
      .collect()
      .map(_.getString(0))
      .toSet
    // The coordinate (t, 0, 4) is shared by dup-a and dup-b: one marked row excludes both. Kafka coordinates are
    // unique in production, so this only documents the key's granularity.
    assertEquals(kept, Set("nulls", "empty", "kept"))
  }

  test("AC-02.1 without Kafka coordinates marker exclusion keys on the event ID and otherwise fails closed") {
    val byEvent = StructType(coordinateSchema.fields.drop(3))
    val frame = tokenRows(
      Seq(
        Row("a", "m", Seq("m")),
        Row("a", "k", Seq("k")),
        Row("b", "k", Seq("k")),
        Row("c", null, null)
      ),
      byEvent
    )
    val kept = AnalyticsSubjectPrivacy
      .excludeActiveDeletionMarkers(frame, markers(Seq("m")))
      .toOption
      .get
      .select("eventId")
      .collect()
      .map(_.getString(0))
      .toList
      .sorted
    assertEquals(kept, List("b", "c"))
    val neither = frame.drop("eventId")
    assertEquals(
      AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(neither, markers(Seq("m"))).left.toOption.map(_.getClass),
      Some(com.example.hiring.analytics.errors.AnalyticsError.InvalidSourceSchema(Vector.empty).getClass)
    )
  }

  test("AC-02.2 the tokenizer runs once per record for one action and never again on the persisted frame") {
    val total = 200
    val events = (0 until total).map(index => statusChanged(s"e$index", s"actor${index % 7}", s"cand${index % 13}"))
    val valid = OperationalEventTransforms.validEvents(parsed(events)).repartition(4)

    OperationalEventTokenCounter.reset()
    val single = AnalyticsSubjectPrivacy.withSubjectToken(valid, OperationalEventTokenCounter.factory)
    // count() alone would prune the unused token columns and skip the function, so read them.
    assertEquals(single.select("subjectToken", "subjectTokens").collect().length, total)
    // Two tokens per record (actor and candidate); both output columns come from one evaluation.
    assertEquals(OperationalEventTokenCounter.tokens.get(), 2 * total)
    assert(
      OperationalEventTokenCounter.tokenizers.get() <= 4,
      s"one tokenizer per task, got ${OperationalEventTokenCounter.tokenizers.get()}"
    )

    OperationalEventTokenCounter.reset()
    val persisted = AnalyticsSubjectPrivacy.withSubjectToken(valid, OperationalEventTokenCounter.factory).persist()
    try {
      assertEquals(persisted.select("subjectToken", "subjectTokens").collect().length, total)
      val afterFirstAction = OperationalEventTokenCounter.tokens.get()
      assertEquals(afterFirstAction, 2 * total)
      val noMarkers =
        AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(persisted, markers(Seq("t-nobody"))).toOption.get
      assertEquals(noMarkers.count(), total.toLong)
      assertEquals(persisted.filter(col("subjectToken").isNotNull).count(), total.toLong)
      assertEquals(persisted.select("subjectTokens").collect().length, total)
      assertEquals(OperationalEventTokenCounter.tokens.get(), afterFirstAction)
    } finally { val _ = persisted.unpersist(blocking = true) }
  }

  test("AC-02.2 Silver does not tokenize a frame that already carries its tokens") {
    val tokenized = AnalyticsSubjectPrivacy.withSubjectToken(
      OperationalEventTransforms.validEvents(parsed(Seq(statusChanged("one", "recruiter", "candidate")))),
      keys
    )
    val persisted = tokenized.persist()
    val silver = OperationalEventTransforms.silver(persisted, keys, markers(Nil)).toOption.get
    // The cached tokens are read as they are: the optimized plan holds no tokenizing function.
    assertEquals(expressions(silver.queryExecution.optimizedPlan).collect { case udf: ScalaUDF => udf }.size, 0)
    val _ = persisted.unpersist(blocking = true)
    val fromBare = OperationalEventTransforms
      .silver(
        OperationalEventTransforms.validEvents(parsed(Seq(statusChanged("one", "recruiter", "candidate")))),
        keys,
        markers(Nil)
      )
      .toOption
      .get
    assertEquals(fromBare.select("subjectToken").head().getString(0), silver.select("subjectToken").head().getString(0))
  }

  test("AC-02.3 marker exclusion plans an equality join, never a nested-loop or Cartesian join") {
    val events = (0 until 300).map(index => statusChanged(s"p$index", s"actor${index % 5}", s"cand${index % 40}"))
    val tokenized = AnalyticsSubjectPrivacy.withSubjectToken(
      OperationalEventTransforms.validEvents(parsed(events)),
      keys
    )
    val active = markers(Seq(token(id("cand3")), token(id("cand7")), token(id("nobody"))))
    val previous = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    try
      Seq("10485760", "-1").foreach { threshold =>
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
        val safe = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(tokenized, active).toOption.get
        val remaining = safe.count()
        val physical = safe.queryExecution.executedPlan.toString
        assert(!physical.contains("BroadcastNestedLoopJoin"), physical)
        assert(!physical.contains("CartesianProduct"), physical)
        assert(physical.contains("Join"), physical)
        assert(remaining < 300L && remaining > 0L)
      }
    finally spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous)
  }

  test("AC-02.3 markers exclude a row whose token is the actor, the candidate or a candidate-search result") {
    val rows = Seq(
      statusChanged("by-actor", "marked-actor", "candidate-1"),
      statusChanged("by-candidate", "recruiter-1", "marked-candidate"),
      search("by-result", "searcher-1", "candidateMatches", Seq("other-result", "marked-result")),
      search("jobs-search-keeps", "searcher-2", "jobs", Seq("marked-result")),
      statusChanged("retained", "recruiter-2", "candidate-2"),
      statusChanged("two-hits", "marked-actor", "marked-candidate")
    )
    val tokenized = AnalyticsSubjectPrivacy.withSubjectToken(OperationalEventTransforms.validEvents(parsed(rows)), keys)
    def survivors(subjects: Seq[String]): Set[String] =
      AnalyticsSubjectPrivacy
        .excludeActiveDeletionMarkers(tokenized, markers(subjects.map(subject => token(id(subject)))))
        .toOption
        .get
        .select("eventId")
        .collect()
        .map(_.getString(0))
        .toSet
    def named(labels: String*): Set[String] = labels.map(id).toSet
    val everyone = named("by-actor", "by-candidate", "by-result", "jobs-search-keeps", "retained", "two-hits")
    assertEquals(survivors(Nil), everyone)
    assertEquals(survivors(Seq("marked-actor")), everyone -- named("by-actor", "two-hits"))
    assertEquals(survivors(Seq("marked-candidate")), everyone -- named("by-candidate", "two-hits"))
    assertEquals(survivors(Seq("marked-result")), everyone - id("by-result"))
    assertEquals(
      survivors(Seq("marked-actor", "marked-candidate", "marked-result")),
      named("jobs-search-keeps", "retained")
    )
    assertEquals(survivors(Seq("unknown")), everyone)
  }

  test("AC-02.3 validation reads the raw envelope with a documented minimum of JSON functions") {
    val frame = OperationalEventTransforms.analyzed(parsed(mixed), keys)
    val validation = expressions(OperationalEventTransforms.withValidity(parsed(mixed)).queryExecution.analyzed)
    val onRaw = (expression: Expression) => isOn(expression, "rawValue")
    // On rawValue: occurredAt and payload extractions, one envelope key-set check, one VARIANT parse. Everything else
    // reads the payload text those extractions produce or the VARIANT, never the raw envelope again.
    assertEquals(validation.count(e => e.isInstanceOf[GetJsonObject] && onRaw(e)), 2)
    assertEquals(validation.count(e => e.isInstanceOf[JsonObjectKeys] && onRaw(e)), 1)
    assertEquals(validation.count(_.prettyName.contains("parse_json")), 1)
    // Over the payload text: job, results and four presence checks, plus payload and job key sets and one key set per
    // result. The previous per-branch expression held 16 get_json_object and 11 json_object_keys calls.
    assertEquals(validation.count(_.isInstanceOf[GetJsonObject]), 8)
    assertEquals(validation.count(_.isInstanceOf[JsonObjectKeys]), 4)
    // On a file-backed source the optimizer keeps the expressions, so the optimized plan has no more calls either.
    val fileBacked = {
      val dir = java.nio.file.Files.createTempDirectory("parsed-frame-spec-").resolve("raw").toString
      rawRecords(mixed).write.parquet(dir)
      OperationalEventTransforms.parseKafkaRecords(spark.read.parquet(dir))
    }
    val optimized = expressions(OperationalEventTransforms.withValidity(fileBacked).queryExecution.optimizedPlan)
    val optimizedCalls = optimized.count(_.isInstanceOf[GetJsonObject])
    assert(optimizedCalls >= 1 && optimizedCalls <= 8, s"get_json_object calls in optimized plan: $optimizedCalls")
    // Consumers read the persisted columns: no JSON function is left in their plans.
    val persisted = frame.persist()
    try {
      persisted.count()
      val consumers = Seq(
        OperationalEventTransforms.validEvents(persisted),
        OperationalEventTransforms.malformedEvents(persisted),
        OperationalEventTransforms.conflictingEventIds(OperationalEventTransforms.validEvents(persisted)),
        OperationalEventTransforms.bronze(OperationalEventTransforms.validEvents(persisted))
      )
      consumers.foreach { consumer =>
        val used = expressions(consumer.queryExecution.optimizedPlan)
        assert(
          !used.exists(e =>
            e.isInstanceOf[GetJsonObject] || e.isInstanceOf[JsonObjectKeys] || e.prettyName.contains("parse_json")
          )
        )
        assert(!used.exists(_.isInstanceOf[ScalaUDF]))
        assert(!used.exists(_.prettyName == "sha2"))
      }
    } finally { val _ = persisted.unpersist(blocking = true) }
  }
}

/** Counts tokenizer constructions and tokenizations. It is top level so the tokenizer constructor serializes alone. */
object OperationalEventTokenCounter {
  val tokenizers = new AtomicInteger(0)
  val tokens = new AtomicInteger(0)
  def reset(): Unit = { tokenizers.set(0); tokens.set(0) }
  val factory: () => String => String = () => {
    val _ = tokenizers.incrementAndGet()
    subject => { val _ = tokens.incrementAndGet(); "t-" + subject }
  }
}
