package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** Store-enforced equivalent of the current cleanup codec and pure retention proof validation. */
private[mongo] object MongoInterviewCleanupValidator {
  import MongoValidatorSchemas.{enumSchema, objectSchema, AnchoredUuidPattern, UuidPattern}

  private val Producer = s"^(hiring-interview-orchestrator-|hiring-interview-worker-)$UuidPattern$$"

  def definition(topics: InterviewTopicPair): Document = {
    def values(operator: String, operands: Any*): Document = new Document(operator, operands.toList.asJava)
    def stateShape(names: List[String], required: List[String]): Document = {
      val shape = new Document("properties", new Document(MongoFields.State, enumSchema(names*)))
      if (required.isEmpty) shape else shape.append("required", required.asJava)
    }
    val barrier = objectSchema(
      List(MongoFields.Topic, MongoFields.Partition, "endOffset"),
      new Document(MongoFields.Topic, enumSchema(topics.commands, topics.results))
        .append(MongoFields.Partition, new Document("bsonType", "int").append("minimum", 0))
        .append("endOffset", new Document("bsonType", "long").append("minimum", Long.box(0L)))
    )
    val schema = objectSchema(
      List(
        MongoFields.Id,
        MongoFields.Revision,
        MongoFields.RequestedAt,
        MongoFields.State,
        MongoFields.InterviewTransactionalIds,
        MongoFields.ProducerRegistry
      ),
      new Document(MongoFields.Id, new Document("bsonType", "string").append("pattern", AnchoredUuidPattern))
        .append(
          MongoFields.Revision,
          new Document("bsonType", "long")
            .append("minimum", Long.box(0L))
            .append("maximum", Long.box(Long.MaxValue - 1L))
        )
        .append(MongoFields.RequestedAt, new Document("bsonType", "date"))
        .append(MongoFields.State, enumSchema((MongoInterviewCleanupSweepCodec.ActiveStates :+ "Complete")*))
        .append(MongoFields.ProducerRegistry, new Document("enum", List(true).asJava))
        .append(
          MongoFields.InterviewTransactionalIds,
          new Document("bsonType", "array")
            .append("items", new Document("bsonType", "string").append("pattern", Producer))
        )
    ).append(
      "oneOf",
      List(
        stateShape(List("AwaitingRetention"), List("barriers"))
          .append(
            "properties",
            new Document(MongoFields.State, enumSchema("AwaitingRetention"))
              .append("barriers", new Document("bsonType", "array").append("minItems", 1).append("items", barrier))
          ),
        stateShape(List("Complete"), List(MongoFields.CompletedAt))
          .append(
            "properties",
            new Document(MongoFields.State, enumSchema("Complete"))
              .append(MongoFields.CompletedAt, new Document("bsonType", "date"))
          ),
        stateShape(List("Pending", "ProducersFenced", "MongoPurged"), Nil)
      ).asJava
    )
    // Validators can evaluate expressions even when the schema rejects a row. Keep every expression total.
    val safeBarriers = values("$cond", new Document("$isArray", "$barriers"), "$barriers", List.empty[Any].asJava)
    def mapBarriers(expression: Any): Document = new Document(
      "$map",
      new Document("input", safeBarriers).append("as", "barrier").append("in", expression)
    )
    val pairs = mapBarriers(
      new Document(MongoFields.Topic, "$$barrier.topic").append(MongoFields.Partition, "$$barrier.partition")
    )
    val proof = values(
      "$and",
      values(
        "$setEquals",
        mapBarriers("$$barrier.topic"),
        new Document("$literal", List(topics.commands, topics.results).asJava)
      ),
      values(
        "$eq",
        new Document("$size", pairs),
        new Document("$size", values("$setUnion", pairs, List.empty[Any].asJava))
      )
    )
    new Document(
      "$and",
      List(
        new Document("$jsonSchema", schema),
        new Document("$expr", values("$cond", values("$eq", "$state", "AwaitingRetention"), proof, true))
      ).asJava
    )
  }
}
