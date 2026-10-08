package com.example.graphQL.cats.repository.mongo

import munit.FunSuite

import scala.jdk.CollectionConverters.*

final class MongoEmbeddingCoverageProjectionSpec extends FunSuite {
  test("ECR-07 entity projections exclude the embedding vector and keep only metadata") {
    List(MongoEmbeddingCoverageRepository.JobScan, MongoEmbeddingCoverageRepository.CandidateScan).foreach { scan =>
      assert(!scan.projection.containsKey(MongoFields.Embedding))
      assert(scan.projection.containsKey(MongoFields.EmbeddingMeta))
      assert(!scan.projection.containsKey(MongoFields.PasswordHash))
      assert(!scan.projection.containsKey(MongoFields.Email))
      assert(scan.projection.values().asScala.forall(_ == Integer.valueOf(1)), "inclusion-only projection")
    }
    val pipeline =
      MongoEmbeddingCoverageRepository.entityPagePipeline(MongoEmbeddingCoverageRepository.JobScan, None, 5)
    assert(
      pipeline.exists(stage =>
        stage.containsKey("$project") && !stage
          .get("$project", classOf[org.bson.Document])
          .containsKey(MongoFields.Embedding)
      )
    )
  }
}
