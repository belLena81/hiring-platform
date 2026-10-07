package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** Store-enforced equivalent of the current cleanup codec and pure retention proof validation. */
private[mongo] object MongoInterviewCleanupValidator {
  private val Uuid = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
  private val Producer = s"^(hiring-interview-orchestrator-|hiring-interview-worker-)$Uuid$$"

  def definition(topics: InterviewTopicPair): Document = {
    def values(operator: String, operands: Any*): Document = new Document(operator, operands.toList.asJava)
    def enumSchema(names: String*): Document = values("enum", names*)
    def objectSchema(required: List[String], properties: Document): Document =
      new Document("bsonType", "object").append("required", required.asJava).append("properties", properties)
    def stateShape(names: List[String], required: List[String]): Document = {
      val shape = new Document("properties", new Document("state", enumSchema(names*)))
      if (required.isEmpty) shape else shape.append("required", required.asJava)
    }
    val barrier = objectSchema(
      List("topic", "partition", "endOffset"),
      new Document("topic", enumSchema(topics.commands, topics.results))
        .append("partition", new Document("bsonType", "int").append("minimum", 0))
        .append("endOffset", new Document("bsonType", "long").append("minimum", Long.box(0L)))
    )
    val schema = objectSchema(
      List("_id", "revision", "requestedAt", "state", "interviewTransactionalIds", "producerRegistry"),
      new Document("_id", new Document("bsonType", "string").append("pattern", s"^$Uuid$$"))
        .append(
          "revision",
          new Document("bsonType", "long")
            .append("minimum", Long.box(0L))
            .append("maximum", Long.box(Long.MaxValue - 1L))
        )
        .append("requestedAt", new Document("bsonType", "date"))
        .append("state", enumSchema((MongoInterviewCleanupSweepCodec.ActiveStates :+ "Complete")*))
        .append("producerRegistry", new Document("enum", List(true).asJava))
        .append(
          "interviewTransactionalIds",
          new Document("bsonType", "array")
            .append("items", new Document("bsonType", "string").append("pattern", Producer))
        )
    ).append(
      "oneOf",
      List(
        stateShape(List("AwaitingRetention"), List("barriers"))
          .append(
            "properties",
            new Document("state", enumSchema("AwaitingRetention"))
              .append("barriers", new Document("bsonType", "array").append("minItems", 1).append("items", barrier))
          ),
        stateShape(List("Complete"), List("completedAt"))
          .append(
            "properties",
            new Document("state", enumSchema("Complete")).append("completedAt", new Document("bsonType", "date"))
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
      new Document("topic", "$$barrier.topic").append("partition", "$$barrier.partition")
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
