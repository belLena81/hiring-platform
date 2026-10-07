package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.Identifiers.parse as parseIdentifier
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO, SemanticSearchRepository}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson

import fs2.Stream
import scala.jdk.CollectionConverters.*

final class MongoSemanticSearchRepository(
    database: MongoDatabase[IO],
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    candidateLexicalIndex: String,
    numCandidates: Int,
    branchResultLimit: Int = 0,
    fusionStrategy: SearchFusionStrategy = SearchFusionStrategy.ApplicationRrf,
    rerankEnabled: Boolean = false,
    rerankModel: String = "rerank-2.5-lite",
    diagnostics: Diagnostics
) extends SemanticSearchRepository {

  private val branchLimit = if (branchResultLimit > 0) branchResultLimit else numCandidates

  private def collectWithin[A](stream: Stream[IO, A], maximum: Int): IO[List[A]] =
    stream.take(maximum.toLong + 1L).compile.toList.flatMap { values =>
      if (values.size > maximum) IO.raiseError(new IllegalStateException("Mongo result exceeded configured limit"))
      else IO.pure(values)
    }

  private def metadataFilter(meta: EmbeddingMeta): Bson = Filters.and(
    Filters.eq(MongoFields.EmbeddingMetaModel, meta.model),
    Filters.eq(s"${MongoFields.EmbeddingMeta}.${MongoFields.SourceHash}", meta.sourceHash),
    Filters.eq(s"${MongoFields.EmbeddingMeta}.${MongoFields.UpdatedAt}", java.util.Date.from(meta.updatedAt))
  )

  override def authorizedJobEligibility(
      scope: HiringReadScope,
      ids: List[JobId],
      queryCandidate: Option[CandidateSearchEligibility]
  ): RepositoryIO[List[JobSearchEligibility]] = {
    val queryGate = queryCandidate.toList.flatMap { candidate =>
      (candidate.profile, candidate.metadata) match {
        case (Some(profile), Some(meta)) =>
          List(
            MongoAuthorizedReadQueries.matching(
              Filters.and(
                metadataFilter(meta),
                Filters.eq(MongoFields.ProfileSkills, profile.skills.toList.sorted.asJava),
                Filters.eq(MongoFields.ProfileExperienceSummary, profile.experienceSummary.orNull)
              )
            )
          )
        case _ => List(MongoAuthorizedReadQueries.matching(Filters.expr(new Document("$eq", List(1, 0).asJava))))
      }
    }
    authorizedEligibility(
      MongoCollections.Users,
      ids.map(_.value.toString),
      List(
        MongoAuthorizedReadQueries.matching(
          Filters.and(
            Filters.eq(MongoFields.Id, scope.userId.value.toString),
            Filters.eq(MongoFields.Role, UserRole.Candidate.toString)
          )
        )
      ) ++
        MongoAuthorizedReadQueries.actor(scope) ++ queryGate,
      MongoCollections.Jobs,
      MongoSearchEligibilityCodecs.jobFields
    )(MongoSearchEligibilityCodecs.job)
  }

  override def authorizedCandidateEligibility(
      scope: HiringReadScope,
      queryJob: JobSearchEligibility,
      ids: List[UserId]
  ): RepositoryIO[List[CandidateSearchEligibility]] = {
    val source = queryJob.job
    val embeddingGate =
      queryJob.metadata.map(metadataFilter).getOrElse(Filters.expr(new Document("$eq", List(1, 0).asJava)))
    val parentGate = Filters.and(
      Filters.eq(MongoFields.Id, source.id.value.toString),
      MongoAuthorizedReadQueries.managedJob(scope),
      Filters.eq(MongoFields.Status, JobStatus.Open.toString),
      embeddingGate,
      Filters.eq(MongoFields.Title, source.title),
      Filters.eq(MongoFields.Description, source.description),
      Filters.eq(MongoFields.Requirements, source.requirements.asJava),
      Filters.eq(MongoFields.Skills, source.skills.toList.sorted.asJava)
    )
    authorizedEligibility(
      MongoCollections.Jobs,
      ids.map(_.value.toString),
      List(MongoAuthorizedReadQueries.matching(parentGate)) ++ MongoAuthorizedReadQueries.actor(scope),
      MongoCollections.Users,
      MongoSearchEligibilityCodecs.candidateFields
    )(MongoSearchEligibilityCodecs.candidate)
  }

  private def authorizedEligibility[A](
      parentCollection: String,
      ids: List[String],
      parentGate: List[Document],
      selectedCollection: String,
      fields: List[String]
  )(decode: Document => Either[RepositoryError, A]): RepositoryIO[List[A]] = {
    val selected = ids.distinct
    if (selected.size > branchLimit) RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    else if (selected.isEmpty) RepositoryIO.fromEither(Right(Nil))
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "semanticSearch.authorizedEligibility") {
        val child = List(
          MongoAuthorizedReadQueries.matching(Filters.in(MongoFields.Id, selected.asJava)),
          new Document("$limit", selected.size),
          new Document("$project", MongoSearchEligibilityCodecs.projection(fields))
        )
        val pipeline = parentGate ++ List(
          new Document(
            "$lookup",
            new Document("from", selectedCollection)
              .append("pipeline", child.asJava)
              .append("as", "eligibleHits")
          ),
          MongoAuthorizedReadQueries.unwind("eligibleHits"),
          MongoAuthorizedReadQueries.replace("eligibleHits")
        )
        RepositoryIO
          .lift(Mongo4catsCollections.documents(database, parentCollection).flatMap { collection =>
            collectWithin(collection.aggregate[Document](pipeline).boundedStream(32), selected.size)
          })
          .subflatMap(_.traverse(decode))
      }(_ => Left(RepositoryError.Unavailable))
  }

  override def jobEligibility(ids: List[JobId]): RepositoryIO[List[JobSearchEligibility]] =
    eligibility(MongoCollections.Jobs, ids.map(_.value.toString), MongoSearchEligibilityCodecs.jobFields)(
      MongoSearchEligibilityCodecs.job
    )

  override def candidateEligibility(ids: List[UserId]): RepositoryIO[List[CandidateSearchEligibility]] =
    eligibility(MongoCollections.Users, ids.map(_.value.toString), MongoSearchEligibilityCodecs.candidateFields)(
      MongoSearchEligibilityCodecs.candidate
    )

  private def eligibility[A](collection: String, ids: List[String], fields: List[String])(
      decode: Document => Either[RepositoryError, A]
  ): RepositoryIO[List[A]] = {
    val selected = ids.distinct
    if (selected.size > branchLimit) RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    else if (selected.isEmpty) RepositoryIO.fromEither(Right(Nil))
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "semanticSearch.eligibility") {
        RepositoryIO
          .lift(Mongo4catsCollections.documents(database, collection).flatMap { source =>
            val pipeline = List(
              new Document("$match", new Document(MongoFields.Id, new Document("$in", selected.asJava))),
              new Document("$limit", selected.size),
              new Document("$project", MongoSearchEligibilityCodecs.projection(fields))
            )
            collectWithin(source.aggregate[Document](pipeline).boundedStream(32), selected.size)
          })
          .subflatMap(_.traverse(decode))
      }(_ => Left(RepositoryError.Unavailable))
  }

  override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[RankedJob]] =
    rankedJobs(query, query.filter)

  override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[RankedJob]] =
    rankedJobs(query, JobSearchFilter(None, Set.empty, None))

  override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[RankedCandidate]] = {
    val filter = candidateFilter(query, includeEmbeddingModel = true)
    val vectorLimit = branchLimit
    val jobVector = candidateVectorSearch(query, query.vector, filter, vectorLimit)
    val result = query.candidateQueryVector match {
      case None                                                                       => jobVector
      case Some(queryVector) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeCandidateFusion(query, queryVector, filter)
      case Some(queryVector) =>
        val queryVectorResults = candidateVectorSearch(query, queryVector, filter, branchLimit)
        val lexicalResults = candidateLexicalSearch(query, filter)
        MongoSemanticSearchResult.fuseCandidates(jobVector, queryVectorResults, lexicalResults, branchLimit)
    }
    result
  }

  private def candidateVectorSearch(
      query: VectorSearchQuery,
      vector: List[Float],
      filter: Bson,
      limit: Int
  ): RepositoryIO[List[RankedCandidate]] = {
    val pipeline = candidateVectorPipeline(vector, filter, limit)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.candidateVector") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
              collectWithin(users.aggregate[Document](pipeline.asScala.toSeq).boundedStream(32), limit)
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private def candidateLexicalSearch(
      query: VectorSearchQuery,
      filter: Bson
  ): RepositoryIO[List[RankedCandidate]] = {
    val pipeline = candidateLexicalPipeline(query, filter)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.candidateLexical") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
              collectWithin(users.aggregate[Document](pipeline.asScala.toSeq).boundedStream(32), branchLimit)
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def candidateFilter(query: VectorSearchQuery, includeEmbeddingModel: Boolean): Bson = {
    val filters = List(
      Some(Filters.eq(MongoFields.Role, UserRole.Candidate.toString)),
      Some(Filters.eq(MongoFields.AccountStatus, AccountStatus.Active.toString)),
      Option.when(includeEmbeddingModel)(Filters.eq(MongoFields.EmbeddingMetaModel, query.model)),
      Option.when(query.candidateFilters.requiredSkills.nonEmpty)(
        Filters.and(
          query.candidateFilters.requiredSkills
            .map(Filters.eq(MongoFields.ProfileSkillsCanonical, _))*
        )
      )
    ).flatten ++ privateCandidateFilters(query.candidateFilters)
    Filters.and(filters*)
  }

  private def privateCandidateFilters(filters: ValidatedCandidateMatchFilters): List[Bson] = {
    val requested = List(
      filters.countryCanonical.map(value => Filters.eq(MongoFields.ProfileCurrentResidenceCountryCanonical, value)),
      filters.cityCanonical.map(value => Filters.eq(MongoFields.ProfileCurrentResidenceCityCanonical, value)),
      filters.availabilityStatus.map(value => Filters.eq(MongoFields.ProfileAvailabilityStatus, value.toString))
    ).flatten
    Option
      .when(requested.nonEmpty)(
        Filters.or(
          Filters.eq(MongoFields.ProfileRecruiterSearchOptIn, false),
          Filters.exists(MongoFields.ProfileRecruiterSearchOptIn, false),
          Filters.and((Filters.eq(MongoFields.ProfileRecruiterSearchOptIn, true) :: requested)*)
        )
      )
      .toList
  }

  private val candidateProjection = new Document(
    "$project",
    new Document(MongoFields.Id, 1)
      .append(MongoFields.Name, 1)
      .append(MongoFields.ProfileSkills, 1)
      .append(MongoFields.ProfileExperienceSummary, 1)
      .append(MongoFields.EmbeddingMeta, 1)
      .append(MongoFields.Score, 1)
      .append(MongoFields.RetrievalScore, 1)
  )

  private def rankedJobs(
      query: VectorSearchQuery,
      filter: JobSearchFilter
  ): RepositoryIO[List[RankedJob]] = {
    val mongoFilter = jobFilter(query, filter)
    query.lexicalQuery.filter(_ => query.mode == SearchMode.HYBRID) match {
      case Some(text) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeJobFusion(query, text, mongoFilter)
      case Some(text) =>
        val vector = vectorJobs(query, mongoFilter, branchLimit)
        val lexical = lexicalJobs(query, text, mongoFilter)
        MongoSemanticSearchResult.fuseJobs(vector, lexical, branchLimit)
      case None => vectorJobs(query, mongoFilter, branchLimit)
    }
  }

  private def nativeJobFusion(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): RepositoryIO[List[RankedJob]] = {
    val stages = nativeJobFusionStages(query, text, filter)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.nativeJobs") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
              collectWithin(jobs.aggregate[Document](stages.asJava.asScala.toSeq).boundedStream(32), branchLimit)
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def nativeJobFusionStages(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("vector", List(vectorSearchStage(jobVectorIndex, query.vector, filter, branchLimit)).asJava)
      .append("lexical", lexicalJobStages(text, filter))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline
        .rerankStage(
          rerankModel,
          text,
          List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills),
          branchLimit
        )
    )
    MongoSearchFusionPipeline.rankedStages(
      fusionStrategy,
      pipelines,
      List("vector", "lexical"),
      branchLimit,
      rerank,
      Nil,
      scoreFieldStage
    )
  }

  private[mongo] def lexicalJobStages(text: String, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", jobLexicalIndex).append(
      "text",
      new Document("query", text).append(
        "path",
        List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills).asJava
      )
    )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", Int.box(branchLimit))
    ).asJava
  }

  private val scoreFieldStage = new Document("$set", new Document(MongoFields.Score, new Document("$meta", "score")))

  private def nativeCandidateFusion(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): RepositoryIO[List[RankedCandidate]] = {
    val pipeline = nativeCandidateFusionStages(query, queryVector, filter)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.nativeCandidates") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
              collectWithin(
                users.aggregate[Document](pipeline.asJava.asScala.toSeq).boundedStream(32),
                branchLimit
              )
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def nativeCandidateFusionStages(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("jobVector", List(vectorSearchStage(candidateVectorIndex, query.vector, filter, branchLimit)).asJava)
      .append("queryVector", List(vectorSearchStage(candidateVectorIndex, queryVector, filter, branchLimit)).asJava)
      .append("lexical", candidateLexicalStages(query, filter))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline.rerankStage(
        rerankModel,
        query.lexicalQuery.getOrElse(""),
        List(MongoFields.ProfileSkills, MongoFields.ProfileExperienceSummary),
        branchLimit
      )
    )
    val rerankPreparation =
      if (rerankEnabled)
        List(
          new Document(
            "$set",
            new Document(
              "profile.experienceSummary",
              new Document("$ifNull", List("$profile.experienceSummary", "").asJava)
            )
          )
        )
      else Nil
    MongoSearchFusionPipeline.rankedStages(
      fusionStrategy,
      pipelines,
      List("jobVector", "queryVector", "lexical"),
      branchLimit,
      rerank,
      rerankPreparation,
      scoreFieldStage
    ) ++ List(candidateProjection)
  }

  private[mongo] def candidateLexicalStages(query: VectorSearchQuery, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", candidateLexicalIndex)
      .append(
        "text",
        new Document("query", query.lexicalQuery.getOrElse(""))
          .append("path", List(MongoFields.ProfileSkills, MongoFields.ProfileExperienceSummary).asJava)
      )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", Int.box(branchLimit))
    ).asJava
  }

  private def vectorJobs(
      query: VectorSearchQuery,
      filter: Bson,
      limit: Int
  ): RepositoryIO[List[RankedJob]] = {
    val pipeline = jobVectorPipeline(query.vector, filter, limit)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.vectorJobs") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
              collectWithin(jobs.aggregate[Document](pipeline.asScala.toSeq).boundedStream(32), limit)
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def jobVectorPipeline(vector: List[Float], filter: Bson, limit: Int): java.util.List[Document] =
    List(vectorSearchStage(jobVectorIndex, vector, filter, limit), scoreStage).asJava

  private[mongo] def candidateVectorPipeline(vector: List[Float], filter: Bson, limit: Int): java.util.List[Document] =
    List(
      vectorSearchStage(candidateVectorIndex, vector, filter, limit),
      scoreStage,
      candidateProjection
    ).asJava

  private[mongo] def candidateLexicalPipeline(query: VectorSearchQuery, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", candidateLexicalIndex)
      .append(
        "text",
        new Document("query", query.lexicalQuery.getOrElse(""))
          .append("path", List(MongoFields.ProfileSkills, MongoFields.ProfileExperienceSummary).asJava)
      )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(branchLimit)),
      new Document("$set", new Document(MongoFields.Score, new Document("$meta", "searchScore"))),
      candidateProjection
    ).asJava
  }

  private def lexicalJobs(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): RepositoryIO[List[RankedJob]] = {
    val search = new Document("index", jobLexicalIndex)
      .append(
        "text",
        new Document("query", text)
          .append(
            "path",
            List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills).asJava
          )
      )
    val pipeline = List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(branchLimit)),
      new Document("$set", new Document(MongoFields.Score, new Document("$meta", "searchScore")))
    ).asJava
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "semanticSearch.lexicalJobs") {
        RepositoryIO
          .lift(
            Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
              collectWithin(jobs.aggregate[Document](pipeline.asScala.toSeq).boundedStream(32), branchLimit)
            }
          )
          .subflatMap(documents => MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten))
      }(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoKeysetPaging.filter(
      List(
        Some(Filters.eq(MongoFields.Status, JobStatus.Open.toString)),
        Some(Filters.eq(MongoFields.EmbeddingMetaModel, query.model)),
        filter.city.map(city => Filters.eq(s"${MongoFields.Location}.${MongoFields.City}", city)),
        skillsFilter(filter.skills),
        filter.createdAfter.map(createdAfter => Filters.gte(MongoFields.CreatedAt, java.util.Date.from(createdAfter)))
      )
    )

  private def skillsFilter(skills: Set[String]): Option[Bson] =
    Option.when(skills.nonEmpty)(Filters.and(skills.toList.sorted.map(skill => Filters.eq(MongoFields.Skills, skill))*))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, limit: Int): Document =
    new Document(
      "$vectorSearch",
      new Document("index", index)
        .append("path", MongoFields.Embedding)
        .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
        .append("numCandidates", java.lang.Integer.valueOf(numCandidates))
        .append("limit", java.lang.Integer.valueOf(limit))
        .append("filter", filter)
    )

  private val scoreStage: Document =
    new Document("$set", new Document(MongoFields.Score, new Document("$meta", "vectorSearchScore")))
}

private[mongo] object MongoSearchFusionPipeline {
  def fusionStage(strategy: SearchFusionStrategy, pipelines: Document, names: List[String]): Document = {
    val weights = names.foldLeft(new Document())((value, name) => value.append(name, 1.0d))
    val input = new Document("pipelines", pipelines)
    strategy match {
      case SearchFusionStrategy.MongoRankFusion =>
        new Document(
          "$rankFusion",
          new Document("input", input)
            .append("combination", new Document("weights", weights))
        )
      case SearchFusionStrategy.MongoScoreFusion =>
        input.append("normalization", "sigmoid")
        new Document(
          "$scoreFusion",
          new Document("input", input)
            .append("combination", new Document("weights", weights).append("method", "avg"))
        )
      case SearchFusionStrategy.ApplicationRrf => new Document()
    }
  }

  def rerankStage(model: String, query: String, paths: List[String], limit: Int): Document =
    new Document(
      "$rerank",
      new Document("query", new Document("text", query))
        .append("path", paths.asJava)
        .append("numDocsToRerank", Int.box(limit))
        .append("model", model)
    )

  def rankedStages(
      strategy: SearchFusionStrategy,
      pipelines: Document,
      names: List[String],
      limit: Int,
      rerank: Option[Document],
      rerankPreparation: List[Document],
      scoreStage: Document
  ): List[Document] = {
    val fusion = fusionStage(strategy, pipelines, names)
    rerank match {
      case Some(stage) =>
        List(
          fusion,
          new Document("$set", new Document(MongoFields.RetrievalScore, new Document("$meta", "score"))),
          new Document("$sort", new Document(MongoFields.RetrievalScore, -1).append(MongoFields.Id, 1)),
          new Document("$limit", Int.box(limit))
        ) ++ rerankPreparation ++ List(stage, scoreStage)
      case None =>
        List(
          fusion,
          scoreStage,
          new Document("$sort", new Document(MongoFields.Score, -1).append(MongoFields.Id, 1)),
          new Document("$limit", Int.box(limit))
        )
    }
  }
}

private[mongo] object MongoSemanticSearchResult {
  def fuseJobs(
      vector: RepositoryIO[List[RankedJob]],
      lexical: RepositoryIO[List[RankedJob]],
      limit: Int
  ): RepositoryIO[List[RankedJob]] =
    (vector, lexical)
      .parMapN((vectorHits, lexicalHits) => HybridRankFusion.jobs(vectorHits, lexicalHits, limit))
      .leftMap(_ => RepositoryError.Unavailable)

  def fuseCandidates(
      jobVector: RepositoryIO[List[RankedCandidate]],
      queryVector: RepositoryIO[List[RankedCandidate]],
      lexical: RepositoryIO[List[RankedCandidate]],
      limit: Int
  ): RepositoryIO[List[RankedCandidate]] =
    (jobVector, queryVector, lexical)
      .parMapN((jobs, text, lexicalHits) => HybridRankFusion.candidates(jobs, text, lexicalHits, limit))
      .leftMap(_ => RepositoryError.Unavailable)

  def rankedJobs(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[RankedJob]]] =
    documents.traverse(rankedJob(_, query)).leftMap(_ => RepositoryError.InvalidStoredData)

  def rankedCandidates(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[RankedCandidate]]] =
    documents.traverse(rankedCandidate(_, query))

  def rankedJob(
      document: Document,
      query: VectorSearchQuery
  ): Either[RepositoryError, Option[RankedJob]] =
    val stored = new Document(document)
    stored.remove(MongoFields.Score)
    stored.remove(MongoFields.RetrievalScore)
    MongoHiringCodecs
      .readJob(stored)
      .toEither
      .leftMap(_ => RepositoryError.InvalidStoredData)
      .flatMap { job =>
        readScore(document, MongoFields.Score, required = true)
          .map(_.getOrElse(0.0d))
          .flatMap { score =>
            readScore(document, MongoFields.RetrievalScore, required = false).map { retrievalScore =>
              for {
                embedding <- job.embedding
                if embedding.meta.model == query.model
                if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))
              } yield RankedJob(
                job,
                score,
                query.mode,
                embedding.meta,
                query.searchId,
                retrievalScore = retrievalScore
              )
            }
          }
      }

  def rankedCandidate(
      document: Document,
      query: VectorSearchQuery
  ): Either[RepositoryError, Option[RankedCandidate]] =
    parseIdentifier(document.getString(MongoFields.Id))(UserId.apply)
      .leftMap(_ => RepositoryError.InvalidStoredData)
      .flatMap { id =>
        readScore(document, MongoFields.Score, required = true)
          .map(_.getOrElse(0.0d))
          .flatMap { score =>
            readScore(document, MongoFields.RetrievalScore, required = false).flatMap { retrievalScore =>
              val decoded = Either.catchNonFatal {
                val name = document.getString(MongoFields.Name)
                val profile = document.get(MongoFields.Profile, classOf[Document])
                val skills = profile.get(MongoFields.Skills, classOf[java.util.List[String]]).asScala.toSet
                val summary = Option(profile.get(MongoFields.ExperienceSummary)).collect { case value: String => value }
                val storedMeta = document.get(MongoFields.EmbeddingMeta, classOf[Document])
                val model = storedMeta.getString(MongoFields.Model)
                val sourceHash = storedMeta.getString(MongoFields.SourceHash)
                val updatedAt = storedMeta.getDate(MongoFields.UpdatedAt).toInstant
                val profileValue = CandidateProfile(skills, summary, None)
                val meta = EmbeddingMeta(model, sourceHash, updatedAt)
                Option.when(
                  model == query.model && sourceHash == SourceHash.sha256(SearchableText.candidate(profileValue))
                ) {
                  RankedCandidate(
                    CandidateSearchHit(id, name, skills, summary.filter(_.nonEmpty)),
                    score,
                    query.mode,
                    meta,
                    query.searchId,
                    retrievalScore = retrievalScore
                  )
                }
              }
              decoded.leftMap(_ => RepositoryError.InvalidStoredData)
            }
          }
      }

  private def readScore(
      document: Document,
      field: String,
      required: Boolean
  ): Either[RepositoryError, Option[Double]] =
    Option(document.get(field)) match {
      case None if required                                    => Left(RepositoryError.InvalidStoredData)
      case None                                                => Right(None)
      case Some(value: Number) if value.doubleValue().isFinite => Right(Some(value.doubleValue()))
      case Some(_)                                             => Left(RepositoryError.InvalidStoredData)
    }
}
