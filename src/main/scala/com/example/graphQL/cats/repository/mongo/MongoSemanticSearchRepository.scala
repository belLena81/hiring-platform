package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.repository.protocol.{RepositoryError, SemanticSearchRepository}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.search.*
import com.mongodb.client.model.Filters
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters.*

final class MongoSemanticSearchRepository(
    database: MongoDatabase,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    numCandidates: Int
) extends SemanticSearchRepository {
  private val jobs = database.getCollection("jobs")
  private val users = database.getCollection("users")

  override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, query.filter)

  override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, JobSearchFilter(None, Set.empty, None))

  override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val filter = Filters.and(
      Filters.eq("role", UserRole.Candidate.toString),
      Filters.exists("profile"),
      Filters.eq("embeddingMeta.model", query.model)
    )
    val pipeline = List(vectorSearchStage(candidateVectorIndex, query.vector, filter, query.first.value), scoreStage).asJava
    PublisherBridge.collectWithin(users.aggregate(pipeline), query.first.value).map { documents =>
      MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def rankedJobs(query: VectorSearchQuery, filter: JobSearchFilter): IO[Either[RepositoryError, List[RankedJob]]] = {
    val mongoFilter = jobFilter(query, filter)
    query.lexicalQuery.filter(_ => query.mode == SearchMode.HYBRID) match {
      case Some(text) =>
        val vector = vectorJobs(query, mongoFilter, numCandidates)
        val lexical = lexicalJobs(query, text, mongoFilter)
        (vector, lexical).parMapN { (vectorResults, lexicalResults) =>
          (vectorResults, lexicalResults) match {
            case (Right(vectorHits), Right(lexicalHits)) =>
              Right(HybridRankFusion.jobs(vectorHits, lexicalHits, query.first.value))
            case _ => Left(RepositoryError.Unavailable)
          }
        }
      case None => vectorJobs(query, mongoFilter, query.first.value)
    }
  }

  private def vectorJobs(query: VectorSearchQuery, filter: Bson, limit: Int): IO[Either[RepositoryError, List[RankedJob]]] = {
    val pipeline = List(vectorSearchStage(jobVectorIndex, query.vector, filter, limit), scoreStage).asJava
    PublisherBridge.collectWithin(jobs.aggregate(pipeline), limit).map { documents =>
      MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def lexicalJobs(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val search = new Document("index", jobLexicalIndex)
      .append("text", new Document("query", text)
        .append("path", List("title", "description", "requirements", "skills").asJava))
    val pipeline = List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(numCandidates)),
      new Document("$set", new Document("score", new Document("$meta", "searchScore")))
    ).asJava
    PublisherBridge.collectWithin(jobs.aggregate(pipeline), numCandidates).map { documents =>
      MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoKeysetPaging.filter(List(
      Some(Filters.eq("status", JobStatus.Open.toString)),
      Some(Filters.eq("embeddingMeta.model", query.model)),
      filter.city.map(city => Filters.eq("location.city", city)),
      skillsFilter(filter.skills),
      filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
    ))

  private def skillsFilter(skills: Set[String]): Option[Bson] =
    Option.when(skills.nonEmpty)(Filters.and(skills.toList.sorted.map(skill => Filters.eq("skills", skill))*))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, limit: Int): Document =
    new Document("$vectorSearch", new Document("index", index)
      .append("path", "embedding")
      .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
      .append("numCandidates", java.lang.Integer.valueOf(numCandidates))
      .append("limit", java.lang.Integer.valueOf(limit))
      .append("filter", filter))

  private val scoreStage: Document =
    new Document("$set", new Document("score", new Document("$meta", "vectorSearchScore")))
}

private[mongo] object MongoSemanticSearchResult {
  def rankedJobs(documents: List[Document], query: VectorSearchQuery): Either[RepositoryError, List[Option[RankedJob]]] =
    documents.traverse(rankedJob(_, query)).leftMap(_ => RepositoryError.Unavailable)

  def rankedCandidates(documents: List[Document], query: VectorSearchQuery): Either[RepositoryError, List[Option[RankedCandidate]]] =
    documents.traverse(rankedCandidate(_, query)).leftMap(_ => RepositoryError.Unavailable)

  def rankedJob(document: Document, query: VectorSearchQuery): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], Option[RankedJob]] =
    val stored = new Document(document)
    stored.remove("score")
    MongoHiringCodecs.readJob(stored).toEither.map { job =>
      for {
      embedding <- job.embedding
      if embedding.meta.model == query.model
      if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))
      score <- Option(document.get("score")).collect { case value: Number => value }
    } yield RankedJob(job, score.doubleValue, query.mode, embedding.meta, query.searchId)
    }

  def rankedCandidate(document: Document, query: VectorSearchQuery): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], Option[RankedCandidate]] =
    val stored = new Document(document)
    stored.remove("score")
    MongoHiringCodecs.readUser(stored).toEither.map { candidate =>
      for {
      profile <- candidate.candidateProfile
      embedding <- candidate.embedding
      if embedding.meta.model == query.model
      if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
      score <- Option(document.get("score")).collect { case value: Number => value }
    } yield RankedCandidate(candidate, score.doubleValue, query.mode, embedding.meta, query.searchId)
    }
}
