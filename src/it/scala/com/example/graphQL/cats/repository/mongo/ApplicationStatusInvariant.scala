package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase

/** AC-04.1: the stored status of an application is the `newStatus` of its latest history row. Every integration test
  * that changes a status, through the API path, the interview hiring commit or an interview cancellation, ends with
  * this check, so a writer that moved one without the other fails wherever it is exercised.
  */
private[mongo] object ApplicationStatusInvariant {

  /** The history of an application in order: oldest first, `_id` as the tie-breaker the history reads use. */
  def history(database: MongoDatabase[IO], applicationId: ApplicationId): IO[List[(String, Option[String], String)]] =
    Mongo4catsCollections
      .documents(database, MongoCollections.ApplicationEvents)
      .flatMap(
        _.find(Filters.eq(MongoFields.ApplicationId, applicationId.value.toString))
          .sort(com.mongodb.client.model.Sorts.ascending(MongoFields.OccurredAt, MongoFields.Id))
          .all
      )
      .map(
        _.toList.map(event =>
          (
            event.getString(MongoFields.Id),
            Option(event.getString(MongoFields.PreviousStatus)),
            event.getString(MongoFields.NewStatus)
          )
        )
      )

  /** Fails unless the stored status equals the latest event; `expectHistory` also requires that a status write
    * happened.
    */
  def assertAgrees(
      database: MongoDatabase[IO],
      applicationId: ApplicationId,
      expectHistory: Boolean = true
  ): IO[Unit] =
    for {
      stored <- MongoRepositoryTestSupport.findOne(
        database,
        MongoCollections.Applications,
        Filters.eq(MongoFields.Id, applicationId.value.toString)
      )
      events <- history(database, applicationId)
      status <- IO.fromOption(stored.map(_.getString(MongoFields.Status)))(
        new AssertionError(s"application ${applicationId.value} is missing")
      )
      _ <- IO.raiseWhen(expectHistory && events.isEmpty)(
        new AssertionError(s"application ${applicationId.value} has a status write without history")
      )
      _ <- events.lastOption.traverse_ { case (_, _, latest) =>
        IO.raiseUnless(latest == status)(
          new AssertionError(s"application ${applicationId.value} is $status but its latest history row says $latest")
        )
      }
      // The history itself is a chain: each row leaves from the status the previous one reached.
      _ <- IO.raiseUnless(
        events.zip(events.drop(1)).forall { case ((_, _, reached), (_, previous, _)) =>
          previous.forall(_ == reached)
        }
      )(new AssertionError(s"the history of application ${applicationId.value} is not a chain: $events"))
    } yield ()
}
