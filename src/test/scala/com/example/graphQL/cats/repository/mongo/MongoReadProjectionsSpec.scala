package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.{AccountStatus, JobStatus, UserRole}
import com.example.graphQL.cats.service.ServiceFixtures
import com.example.graphQL.cats.service.port.{ActorRecord, JobOwner}
import com.mongodb.MongoClientSettings
import munit.FunSuite
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

final class MongoReadProjectionsSpec extends FunSuite {
  private def render(projection: Bson): Map[String, Any] =
    projection.toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry).asScala.toMap.map {
      (key, value) => key -> value.asInt32().getValue
    }

  private val vectorAndCredential = Set(MongoFields.Embedding, MongoFields.EmbeddingMeta, MongoFields.PasswordHash)

  test("AC-01.1 the actor projection names only the authorization fields and no vector or credential") {
    val included = render(MongoReadProjections.actor)
    assertEquals(
      included.keySet,
      Set(MongoFields.Id, MongoFields.Role, MongoFields.AccountStatus, MongoFields.AdminSingletonKey)
    )
    assert(included.values.forall(_ == 1))
  }

  test("AC-01.1 profile and list reads of users exclude the embedding, its metadata and the credential") {
    assertEquals(render(MongoReadProjections.userProfile).keySet, vectorAndCredential)
    assert(render(MongoReadProjections.userProfile).values.forall(_ == 0))
    assertEquals(
      MongoReadProjections.userProfileStage.get("$project", classOf[Document]).keySet.asScala.toSet,
      vectorAndCredential
    )
  }

  test("AC-01.1 only the embedding pipeline's user read keeps the vector, and still never the credential") {
    assertEquals(render(MongoReadProjections.userWithEmbedding).keySet, Set(MongoFields.PasswordHash))
  }

  test("AC-01.1 job display reads and related-job lookups exclude the embedding and its metadata") {
    val excluded = Set(MongoFields.Embedding, MongoFields.EmbeddingMeta)
    assertEquals(render(MongoReadProjections.jobDisplay).keySet, excluded)
    assertEquals(
      MongoReadProjections.jobDisplayStage.get("$project", classOf[Document]).keySet.asScala.toSet,
      excluded
    )
  }

  test("AC-01.1 the owner projection names only the owner, the recruiter and the status of a job") {
    assertEquals(
      render(MongoReadProjections.jobOwner).keySet,
      Set(MongoFields.Id, MongoFields.RecruiterId, MongoFields.Status)
    )
  }

  test("AC-01.2 an actor projection decodes to the actor record, with the singleton marker only for the Admin key") {
    val admin = new Document(MongoFields.Id, ServiceFixtures.adminId.value.toString)
      .append(MongoFields.Role, "Admin")
      .append(MongoFields.AccountStatus, "Active")
      .append(MongoFields.AdminSingletonKey, "singleton-admin")
    val recruiter = new Document(MongoFields.Id, ServiceFixtures.recruiterId.value.toString)
      .append(MongoFields.Role, "Recruiter")
      .append(MongoFields.AccountStatus, "Deleted")
    assertEquals(
      MongoAuthorizationProjectionCodecs.readActor(admin).toEither,
      Right(ActorRecord(ServiceFixtures.adminId, UserRole.Admin, AccountStatus.Active, adminSingleton = true))
    )
    assertEquals(
      MongoAuthorizationProjectionCodecs.readActor(recruiter).toEither,
      Right(ActorRecord(ServiceFixtures.recruiterId, UserRole.Recruiter, AccountStatus.Deleted, adminSingleton = false))
    )
  }

  test("AC-01.2 an actor projection fails closed when the store returns a field outside the projection") {
    val leaked = new Document(MongoFields.Id, ServiceFixtures.recruiterId.value.toString)
      .append(MongoFields.Role, "Recruiter")
      .append(MongoFields.AccountStatus, "Active")
      .append(MongoFields.PasswordHash, "secret")
    val missingRole = new Document(MongoFields.Id, ServiceFixtures.recruiterId.value.toString)
      .append(MongoFields.AccountStatus, "Active")
    assert(MongoAuthorizationProjectionCodecs.readActor(leaked).isInvalid)
    assert(MongoAuthorizationProjectionCodecs.readActor(missingRole).isInvalid)
  }

  test("AC-01.2 an owner projection decodes the job owner and fails closed on an unknown status or extra field") {
    val owner = new Document(MongoFields.Id, ServiceFixtures.jobId.value.toString)
      .append(MongoFields.RecruiterId, ServiceFixtures.recruiterId.value.toString)
      .append(MongoFields.Status, "Open")
    assertEquals(
      MongoAuthorizationProjectionCodecs.readOwner(owner).toEither,
      Right(JobOwner(ServiceFixtures.jobId, ServiceFixtures.recruiterId, JobStatus.Open))
    )
    assert(
      MongoAuthorizationProjectionCodecs.readOwner(new Document(owner).append(MongoFields.Status, "Paused")).isInvalid
    )
    assert(
      MongoAuthorizationProjectionCodecs
        .readOwner(new Document(owner).append(MongoFields.Embedding, List(1.0).asJava))
        .isInvalid
    )
  }
}
