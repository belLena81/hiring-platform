package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import com.example.hiring.testing.LocalTestServices
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite

/** Suite-owned service with a fresh resource-owned database for every scenario. */
abstract class AnalyticsMongoIntegrationSuite extends CatsEffectSuite {
  protected def dedicatedMongo: Boolean = false
  private val endpoint = ResourceSuiteLocalFixture(
    "analytics-mongo-endpoint",
    LocalTestServices.mongoEndpoint(dedicatedMongo, testCommands = dedicatedMongo)
  )
  private val databaseFixture = ResourceTestLocalFixture(
    "analytics-owned-database",
    Resource.eval(IO.delay(endpoint())).flatMap { service =>
      MongoClient.fromConnectionString[IO](service.uri).flatMap(LocalTestServices.database)
    }
  )
  override def munitFixtures: List[munit.AnyFixture[?]] = List(endpoint, databaseFixture)
  protected def mongoEndpoint: LocalTestServices.MongoEndpoint = endpoint()
  protected def endpointUri: String = mongoEndpoint.uri
  protected def testDatabaseName: String = databaseFixture().underlying.getName
  protected def testDatabase: MongoDatabase[IO] = databaseFixture()
}
