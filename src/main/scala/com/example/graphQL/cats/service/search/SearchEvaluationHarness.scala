package com.example.graphQL.cats.service.search

import io.circe.Json

/** Captured paired Atlas observations consumed by the bounded search evaluation report. */
final case class SearchEvaluationQuery(
    queryId: String,
    relevantIds: Set[String],
    annIds: List[String],
    ennIds: List[String],
    latencyMillis: Double,
    ennLatencyMillis: Option[Double],
    error: Option[String]
)

final case class SearchEvaluationRun(
    strategy: String,
    datasetDocuments: Int,
    filters: Json,
    embeddingModel: String,
    embeddingDimensions: Int,
    quantization: String,
    numCandidates: Int,
    pageSize: Int,
    concurrency: Int,
    durationMillis: Long,
    warmupQueries: Int,
    temperature: String,
    timestampUtc: String,
    environment: String,
    atlasVersion: String,
    collectionIndexBytes: Option[Long],
    vectorSearchIndexBytes: Option[Long],
    cpuMillis: Option[Long],
    peakMemoryBytes: Option[Long],
    providerRequests: Option[Long],
    queryMetrics: Json,
    queries: List[SearchEvaluationQuery]
)

object SearchEvaluationHarness {
  def report(run: SearchEvaluationRun, k: Int): Json = {
    val successful = run.queries.filter(_.error.isEmpty)
    def percentiles(latencies: List[Double]): Json = {
      val sorted = latencies.sorted
      def percentile(p: Double): Json =
        sorted.lift(math.max(0, math.ceil(p * sorted.size).toInt - 1)).fold(Json.Null)(Json.fromDoubleOrNull)
      Json.obj("p50" -> percentile(0.50), "p95" -> percentile(0.95), "p99" -> percentile(0.99))
    }
    val recall = successful.map(query => SearchEvaluationMetrics.recallAtK(query.annIds, query.ennIds.toSet, k))
    val ndcg = successful.map(query => SearchEvaluationMetrics.ndcgAtK(query.annIds, query.relevantIds, k))
    def mean(values: List[Double]): Json =
      if (values.isEmpty) Json.Null else Json.fromDoubleOrNull(values.sum / values.size.toDouble)
    val throughput =
      if (run.durationMillis <= 0L) Json.Null
      else Json.fromDoubleOrNull(successful.size.toDouble * 1000.0 / run.durationMillis.toDouble)

    Json.obj(
      "strategy" -> Json.fromString(run.strategy),
      "workload" -> Json.obj(
        "datasetDocuments" -> Json.fromInt(run.datasetDocuments),
        "filters" -> run.filters,
        "embeddingModel" -> Json.fromString(run.embeddingModel),
        "embeddingDimensions" -> Json.fromInt(run.embeddingDimensions),
        "quantization" -> Json.fromString(run.quantization),
        "numCandidates" -> Json.fromInt(run.numCandidates),
        "pageSize" -> Json.fromInt(run.pageSize),
        "queryCount" -> Json.fromInt(run.queries.size),
        "concurrency" -> Json.fromInt(run.concurrency),
        "warmupQueries" -> Json.fromInt(run.warmupQueries),
        "temperature" -> Json.fromString(run.temperature),
        "timestampUtc" -> Json.fromString(run.timestampUtc),
        "durationMillis" -> Json.fromLong(run.durationMillis),
        "k" -> Json.fromInt(k)
      ),
      "environment" -> Json.obj(
        "name" -> Json.fromString(run.environment),
        "atlasVersion" -> Json.fromString(run.atlasVersion),
        "collectionIndexBytes" -> run.collectionIndexBytes.fold(Json.Null)(Json.fromLong),
        "vectorSearchIndexBytes" -> run.vectorSearchIndexBytes.fold(Json.Null)(Json.fromLong),
        "cpuMillis" -> run.cpuMillis.fold(Json.Null)(Json.fromLong),
        "peakMemoryBytes" -> run.peakMemoryBytes.fold(Json.Null)(Json.fromLong),
        "providerRequests" -> run.providerRequests.fold(Json.Null)(Json.fromLong),
        "queryMetrics" -> run.queryMetrics
      ),
      "measurements" -> Json.obj(
        "successfulQueries" -> Json.fromInt(successful.size),
        "errors" -> Json.fromInt(run.queries.size - successful.size),
        "throughputQueriesPerSecond" -> throughput,
        "latencyMillis" -> Json.obj(
          "ann" -> percentiles(successful.map(_.latencyMillis)),
          "enn" -> percentiles(successful.flatMap(_.ennLatencyMillis))
        ),
        "meanRecallAtKAgainstEnn" -> mean(recall),
        "meanNdcgAtKAgainstJudgments" -> mean(ndcg)
      ),
      "queries" -> Json.arr(run.queries.map { query =>
        Json.obj(
          "queryId" -> Json.fromString(query.queryId),
          "error" -> query.error.fold(Json.Null)(Json.fromString),
          "latencyMillis" -> Json.fromDoubleOrNull(query.latencyMillis),
          "ennLatencyMillis" -> query.ennLatencyMillis.fold(Json.Null)(Json.fromDoubleOrNull),
          "recallAtKAgainstEnn" -> Json.fromDoubleOrNull(
            SearchEvaluationMetrics.recallAtK(query.annIds, query.ennIds.toSet, k)
          ),
          "ndcgAtKAgainstJudgments" -> Json.fromDoubleOrNull(
            SearchEvaluationMetrics.ndcgAtK(query.annIds, query.relevantIds, k)
          )
        )
      }*)
    )
  }
}
