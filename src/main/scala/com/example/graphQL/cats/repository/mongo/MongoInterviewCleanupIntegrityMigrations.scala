package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{Filters, Sorts, Updates, UpdateOptions}
import org.bson.{BsonValue, Document}
import scala.jdk.CollectionConverters.*

/** One audited baseline plus strict store validation replaces repeated cleanup full scans on restart. */
private[mongo] object MongoInterviewCleanupIntegrityMigrations {
  val MigrationId = "015_interview_cleanup_integrity"
  private val BatchSize = 500

  private def validLedger(row: Document): Boolean =
    Option(row.get("version")).contains(Long.box(1L)) &&
      Option(row.get("version")).exists(_.isInstanceOf[java.lang.Long]) &&
      Option(row.get("state")).exists(Set[Any]("Running", "Complete"))

  private def covered(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    List(MongoInterviewCleanupMigrations.MigrationId, MongoProducerRegistrationMigrations.MigrationId).traverse_ { id =>
      database.getCollection(MongoCollections.HiringMigrationLedger).find(Filters.eq("_id", id)).first.flatMap {
        case Some(row) if validLedger(row) && row.get("state") == "Complete" => IO.unit
        case _ => IO.raiseError(new IllegalStateException("Unsupported covered cleanup migration proof"))
      }
    }

  private def definitionMatches(database: MongoHiringSetup.SetupDatabase, topics: InterviewTopicPair): IO[Boolean] = {
    val command = new Document("listCollections", 1)
      .append("filter", new Document("name", MongoCollections.InterviewSubjectCleanup))
    IO.delay(database.underlying.underlying.runCommand(command, classOf[Document]))
      .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)
      .map { result =>
        Option(result.get("cursor", classOf[Document])).toList
          .flatMap(cursor => Option(cursor.getList("firstBatch", classOf[Document])).toList.flatMap(_.asScala))
          .exists { entry =>
            Option(entry.get("options", classOf[Document])).exists { options =>
              Option(options.get("validationLevel")).contains("strict") &&
              Option(options.get("validationAction")).contains("error") &&
              Option(options.get("validator", classOf[Document]))
                .contains(MongoInterviewCleanupValidator.definition(topics))
            }
          }
      }
  }

  def trusted(database: MongoHiringSetup.SetupDatabase, topics: InterviewTopicPair): IO[Boolean] =
    IO.raiseUnless(topics.valid)(new IllegalStateException("Invalid interview cleanup topic identities")) *>
      database
        .getCollection(MongoCollections.HiringMigrationLedger)
        .find(Filters.eq("_id", MigrationId))
        .first
        .flatMap {
          case None                                                            => IO.pure(false)
          case Some(row) if validLedger(row) && row.get("state") == "Running"  => IO.pure(false)
          case Some(row) if validLedger(row) && row.get("state") == "Complete" =>
            definitionMatches(database, topics).flatMap {
              case true  => covered(database).as(true)
              case false =>
                IO.raiseError(
                  new IllegalStateException(
                    "Interview cleanup validator or topic identities changed; explicit maintenance repair required"
                  )
                )
            }
          case _ => IO.raiseError(new IllegalStateException("Unsupported interview cleanup integrity migration proof"))
        }

  def initialize(database: MongoHiringSetup.SetupDatabase, topics: InterviewTopicPair): IO[Unit] =
    trusted(database, topics).flatMap {
      case true  => IO.unit
      case false =>
        val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
        val queue = database.getCollection(MongoCollections.InterviewSubjectCleanup)
        val marker = Filters.eq("_id", MigrationId)
        def acknowledged(result: com.mongodb.client.result.UpdateResult): IO[Unit] =
          IO.raiseUnless(result.wasAcknowledged())(
            new IllegalStateException("Cleanup integrity proof write was not acknowledged")
          )
        def scan(after: Option[BsonValue]): IO[Unit] =
          queue
            .find(MongoInterviewCleanupSweepCodec.afterFilter(after))
            .sort(Sorts.ascending("_id"))
            .hint("_id_")
            .limit(BatchSize)
            .all
            .flatMap { rows =>
              rows.toList.traverse_(row =>
                IO.fromEither(
                  MongoInterviewCleanupCodec
                    .decodeCurrent(row, topics)
                    .leftMap(_ => new IllegalStateException("Cleanup integrity cutover rejected stored evidence"))
                ).void
              ) *> rows.lastOption.traverse_ { last =>
                IO.fromEither(
                  MongoInterviewCleanupSweepCodec
                    .identity(last)
                    .leftMap(_ => new IllegalStateException("Cleanup integrity identity is absent"))
                ).flatMap { identity =>
                  ledger.updateOne(marker, Updates.set("lastId", last.get("_id"))).flatMap(acknowledged) *>
                    (if (rows.size == BatchSize) IO.defer(scan(Some(identity))) else IO.unit)
                }
              }
            }
        for {
          _ <- covered(database)
          _ <- ledger
            .updateOne(
              marker,
              Updates.combine(
                Updates.setOnInsert("version", Long.box(1L)),
                Updates.setOnInsert("state", "Running")
              ),
              new UpdateOptions().upsert(true)
            )
            .flatMap(acknowledged)
            .handleErrorWith {
              case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
              case error                                                         => IO.raiseError(error)
            }
          previous <- ledger.find(marker).first
          _ <- IO.raiseUnless(previous.exists(validLedger))(
            new IllegalStateException("Invalid cleanup integrity checkpoint")
          )
          after <- IO.fromEither(previous.flatMap(row => Option(row.get("lastId"))).traverse { value =>
            MongoInterviewCleanupSweepCodec
              .identity(new Document("_id", value))
              .leftMap(_ => new IllegalStateException("Invalid cleanup integrity identity checkpoint"))
          })
          _ <- database.createCollection(MongoCollections.InterviewSubjectCleanup).recoverWith {
            case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
          }
          _ <- database.runCommand(
            new Document("collMod", MongoCollections.InterviewSubjectCleanup)
              .append("validator", MongoInterviewCleanupValidator.definition(topics))
              .append("validationLevel", "strict")
              .append("validationAction", "error")
          )
          _ <- scan(after)
          // collMod does not retroactively validate old rows; check the full native predicate before proof completion.
          invalid <- queue
            .find(new Document("$nor", List(MongoInterviewCleanupValidator.definition(topics)).asJava))
            .limit(1)
            .first
          _ <- IO.raiseUnless(invalid.isEmpty)(
            new IllegalStateException("Cleanup integrity stored verification failed")
          )
          _ <- ledger
            .updateOne(
              Filters.and(marker, Filters.eq("version", Long.box(1L))),
              Updates.combine(
                Updates.set("state", "Complete"),
                Updates.unset("lastId")
              )
            )
            .flatMap(acknowledged)
          _ <- trusted(database, topics).flatMap(value =>
            IO.raiseUnless(value)(new IllegalStateException("Cleanup integrity proof did not complete"))
          )
        } yield ()
    }
}
