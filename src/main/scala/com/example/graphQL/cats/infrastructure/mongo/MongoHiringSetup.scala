package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.IO
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.Date

object MongoHiringSetup {
  val UsersEmailIndex = "users_emailCanonical_unique"
  val UsersAdminSingletonIndex = "users_adminSingleton_unique"
  val ApplicationsCandidateJobIndex = "applications_candidate_job_unique"
  val JobsRecruiterStatusCreatedIndex = "jobs_recruiter_status_created_id"
  val ApplicationsCandidateStatusCreatedIndex = "applications_candidate_status_created_id"
  val ApplicationsCandidateCreatedIndex = "applications_candidate_created_id"
  val ApplicationsJobStatusCreatedIndex = "applications_job_status_created_id"
  val ApplicationsJobCreatedIndex = "applications_job_created_id"
  val ApplicationEventsApplicationCreatedIndex = "application_events_application_created_id"
  val JobsCreatedIndex = "jobs_created_id"
  val JobsOpenCreatedIndex = "jobs_open_created_id"
  val JobsOpenCityCreatedIndex = "jobs_open_city_created_id"
  val HiringDomainMongoMigrationId = "phase-2-domain-mongodb-v1"
  val HiringGraphQLSearchIndexMigrationId = "hiring-graphql-search-indexes-v1"
  val HiringAdminJobListingIndexMigrationId = "hiring-admin-job-listing-indexes-v1"

  def initialize(database: MongoDatabase): IO[Unit] =
    List(
      createIndex(database.getCollection("users"),
        Indexes.ascending("emailCanonical"), new IndexOptions().name(UsersEmailIndex).unique(true)),
      createIndex(database.getCollection("users"),
        Indexes.ascending("adminSingletonKey"),
        new IndexOptions().name(UsersAdminSingletonIndex).unique(true)
          .partialFilterExpression(Filters.eq("role", "Admin"))),
      createIndex(database.getCollection("applications"),
        Indexes.ascending("candidateId", "jobId"), new IndexOptions().name(ApplicationsCandidateJobIndex).unique(true)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("recruiterId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsRecruiterStatusCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("candidateId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsCandidateStatusCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsCandidateCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("jobId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsJobStatusCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("jobId"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsJobCreatedIndex)),
      createIndex(database.getCollection("application_events"),
        Indexes.compoundIndex(Indexes.ascending("applicationId"), Indexes.descending("occurredAt", "_id")),
        new IndexOptions().name(ApplicationEventsApplicationCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.descending("createdAt", "_id"),
        new IndexOptions().name(JobsCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsOpenCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("status", "location.city"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsOpenCityCreatedIndex))
    ).sequence_.flatMap { _ =>
      val migrations = database.getCollection("schema_migrations")
      recordMigration(migrations, HiringDomainMongoMigrationId, "Hiring domain MongoDB collections and indexes") *>
        recordMigration(migrations, HiringGraphQLSearchIndexMigrationId, "Hiring GraphQL job search indexes") *>
        recordMigration(migrations, HiringAdminJobListingIndexMigrationId, "Hiring Admin job listing indexes")
    }

  private def recordMigration(
      migrations: com.mongodb.reactivestreams.client.MongoCollection[Document],
      id: String,
      description: String
  ): IO[Unit] =
    PublisherBridge.first(migrations.updateOne(
      Filters.eq("_id", id),
      Updates.combine(
        Updates.setOnInsert("_id", id),
        Updates.setOnInsert("schemaVersion", 1),
        Updates.setOnInsert("appliedAt", Date.from(Instant.now())),
        Updates.setOnInsert("description", description),
        Updates.setOnInsert("checksum", id)
      ),
      new UpdateOptions().upsert(true)
    )).void

  private def createIndex(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      keys: org.bson.conversions.Bson,
      options: IndexOptions
  ): IO[Unit] =
    PublisherBridge.first(collection.createIndex(keys, options)).void
}
