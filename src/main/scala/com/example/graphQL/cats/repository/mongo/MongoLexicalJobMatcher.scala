package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Identifiers.parse as parseIdentifier
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.{LexicalJobMatcher, RepositoryIO}
import com.example.graphQL.cats.service.search.{
  JobSearchFilter,
  LexicalJobMatches,
  ScoredJobId,
  SearchEligibilityCriteria
}
import mongo4cats.database.MongoDatabase
import org.bson.Document

/** Reads one bounded window of job IDs and scores from the job lexical index (`$search`, then `$limit`, `$project`).
  * Only the ID and score are projected; query text is the value of a fixed-path `text.query` and is never logged.
  */
final class MongoLexicalJobMatcher(
    database: MongoDatabase[IO],
    jobLexicalIndex: String,
    diagnostics: Diagnostics,
    policy: DiscoveryQueryPolicy
) extends LexicalJobMatcher {

  override def matches(text: String, filter: JobSearchFilter, limit: Int): RepositoryIO[LexicalJobMatches] = {
    val window = limit + 1
    val pipeline = MongoLexicalJobMatcher.pipeline(jobLexicalIndex, text, filter, window)
    MongoRepositorySupport.repositoryGuard(diagnostics, "MongoLexicalJobMatcher.matches")(
      RepositoryIO
        .lift(
          policy.run(
            Mongo4catsCollections
              .documents(database, MongoCollections.Jobs)
              .flatMap(jobs =>
                MongoBoundedResults.collect(
                  jobs.aggregate[Document](pipeline).maxTime(policy.maxTime).boundedStream(32),
                  window,
                  MongoSetupError("Lexical job match window exceeded its limit")
                )
              )
          )
        )
        .subflatMap(documents => MongoLexicalJobMatcher.decode(documents).map(LexicalJobMatches.fromWindow(_, limit)))
    )
  }
}

private[mongo] object MongoLexicalJobMatcher {
  val ScoreField = "score"

  def pipeline(index: String, text: String, filter: JobSearchFilter, window: Int): List[Document] =
    List(
      MongoLexicalSearch.stage(
        index,
        text,
        List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills),
        MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.openJobs(filter))
      ),
      new Document("$limit", Int.box(window)),
      new Document(
        "$project",
        new Document(MongoFields.Id, 1).append(ScoreField, new Document("$meta", "searchScore"))
      )
    )

  def decode(documents: List[Document]): Either[RepositoryError, List[ScoredJobId]] =
    documents.traverse { document =>
      (
        Option(document.get(MongoFields.Id))
          .collect { case value: String => value }
          .flatMap(parseIdentifier(_)(JobId(_))),
        Option(document.get(ScoreField)).collect { case value: Number => value.doubleValue() }
      ).mapN(ScoredJobId.apply).toRight(RepositoryError.InvalidStoredData)
    }
}
