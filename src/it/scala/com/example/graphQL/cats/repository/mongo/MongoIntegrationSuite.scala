package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.example.hiring.testing.LocalTestServices
import munit.CatsEffectSuite

/** Each suite owns a service lifetime; every case owns an independent database and listener. */
abstract class MongoIntegrationSuite extends CatsEffectSuite {
  protected def dedicatedMongo: Boolean = false
  private val endpointFixture =
    ResourceSuiteLocalFixture("hiring-mongo-endpoint", MongoAccessEvaluationSupport.endpoint(dedicatedMongo))
  override def munitFixtures: List[munit.AnyFixture[?]] = List(endpointFixture)
  protected def mongoEndpoint: LocalTestServices.MongoEndpoint = endpointFixture()
  protected def mongoResource: Resource[IO, MongoAccessEvaluationSupport.Fixture] =
    MongoAccessEvaluationSupport.resource(mongoEndpoint)
}
