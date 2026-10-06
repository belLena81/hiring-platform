package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{AccountStatus, JobStatus, UserRole}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.read.HiringReadScope
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.MongoClientSettings
import com.mongodb.client.model.Filters
import scala.jdk.CollectionConverters.*

/** Authorization remains in the same database selection as the related values. */
private[mongo] object MongoAuthorizedReadQueries {
  def matching(filter: Bson): Document =
    new Document("$match", filter.toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry))

  def lookup(from: String, local: String, foreign: String, as: String, pipeline: List[Document] = Nil): Document = {
    val definition =
      new Document("from", from).append("localField", local).append("foreignField", foreign).append("as", as)
    if (pipeline.nonEmpty) {
      val _ = definition.append("pipeline", pipeline.asJava)
    }
    new Document("$lookup", definition)
  }

  def unwind(field: String): Document = new Document("$unwind", s"$$$field")
  def replace(field: String): Document = new Document("$replaceWith", s"$$$field")

  def actor(scope: HiringReadScope): List[Document] = {
    val predicates = List(
      Filters.eq(MongoFields.Id, scope.userId.value.toString),
      Filters.eq(MongoFields.Role, scope.role.toString),
      Filters.eq(MongoFields.AccountStatus, AccountStatus.Active.toString)
    ) ++ Option.when(scope.role == UserRole.Admin)(Filters.eq(MongoFields.AdminSingletonKey, "singleton-admin")).toList
    List(
      new Document(
        "$lookup",
        new Document("from", MongoCollections.Users)
          .append(
            "pipeline",
            List(matching(Filters.and(predicates*)), new Document("$project", new Document(MongoFields.Id, 1))).asJava
          )
          .append("as", "readActor")
      ),
      matching(Filters.exists("readActor.0")),
      new Document("$unset", "readActor")
    )
  }

  def managedJob(scope: HiringReadScope): Bson = scope.role match {
    case UserRole.Recruiter => Filters.eq(MongoFields.RecruiterId, scope.userId.value.toString)
    case UserRole.Admin     => new Document()
    case UserRole.Candidate => Filters.expr(new Document("$eq", List(1, 0).asJava))
  }

  def applicationAccess(scope: HiringReadScope): List[Document] = scope.role match {
    case UserRole.Candidate => List(matching(Filters.eq(MongoFields.CandidateId, scope.userId.value.toString)))
    case UserRole.Admin     => Nil
    case UserRole.Recruiter =>
      List(
        lookup(
          MongoCollections.Jobs,
          MongoFields.JobId,
          MongoFields.Id,
          "ownedJob",
          List(matching(managedJob(scope)), new Document("$project", new Document(MongoFields.Id, 1)))
        ),
        matching(Filters.exists("ownedJob.0")),
        new Document("$unset", "ownedJob")
      )
  }

  def jobAccess(scope: HiringReadScope): List[Document] = scope.role match {
    case UserRole.Admin     => Nil
    case UserRole.Recruiter =>
      List(matching(Filters.or(Filters.eq(MongoFields.Status, JobStatus.Open.toString), managedJob(scope))))
    case UserRole.Candidate =>
      List(
        lookup(
          MongoCollections.Applications,
          MongoFields.Id,
          MongoFields.JobId,
          "ownApplication",
          List(
            matching(Filters.eq(MongoFields.CandidateId, scope.userId.value.toString)),
            new Document("$limit", 1),
            new Document("$project", new Document(MongoFields.Id, 1))
          )
        ),
        matching(
          Filters.or(Filters.eq(MongoFields.Status, JobStatus.Open.toString), Filters.exists("ownApplication.0"))
        ),
        new Document("$unset", "ownApplication")
      )
  }

  def documents(
      database: MongoDatabase[IO],
      collection: String,
      pipeline: List[Document],
      limit: Int,
      diagnostics: Diagnostics
  ): RepositoryIO[List[Document]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "repository.authorizedRead")(
      RepositoryIO.lift(
        Mongo4catsCollections
          .documents(database, collection)
          .flatMap(
            _.aggregate[Document](pipeline :+ new Document("$limit", limit)).boundedStream(limit).compile.toList
          )
      )
    )(_ => Left(RepositoryError.Unavailable))
}
