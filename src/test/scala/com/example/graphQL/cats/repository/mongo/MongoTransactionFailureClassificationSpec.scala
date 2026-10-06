package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.port.RepositoryError
import com.mongodb.{MongoCommandException, MongoException, ServerAddress}
import org.bson.BsonDocument
import munit.FunSuite

final class MongoTransactionFailureClassificationSpec extends FunSuite {
  test("interview transactions classify exhausted driver write conflicts as uncertain and preserve other callers") {
    val codeConflict = new MongoCommandException(
      BsonDocument.parse("""{"ok":0,"code":112,"errmsg":"write conflict"}"""),
      new ServerAddress()
    )
    val labelled = new MongoException("retryable transaction failure")
    labelled.addLabel("TransientTransactionError")
    List(codeConflict, labelled).foreach { error =>
      assertEquals(
        MongoTransactionRunner.mapWrite[Unit](error, RepositoryError.Conflict, RepositoryError.Unavailable),
        Left(RepositoryError.Unavailable)
      )
      assertEquals(
        MongoTransactionRunner.mapWrite[Unit](error, RepositoryError.Conflict),
        Left(RepositoryError.Conflict)
      )
    }
  }

  test("unknown transaction completion is unavailable rather than a confirmed business rejection") {
    val unknown = new MongoException("unknown transaction completion")
    unknown.addLabel("UnknownTransactionCommitResult")
    assertEquals(
      MongoTransactionRunner.mapWrite[Unit](unknown, RepositoryError.Conflict, RepositoryError.Unavailable),
      Left(RepositoryError.Unavailable)
    )
  }
}
