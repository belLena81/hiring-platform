package com.example.graphQL.cats

import cats.effect._
import cats.implicits._
import com.example.graphQL.cats.daos.Dao
import doobie._
import doobie.hikari._
import doobie.util.ExecutionContexts
import doobie.implicits._
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.blaze.server._
import sangria.execution._
import sangria.macros.derive._
import sangria.parser._
import sangria.schema._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

case class Company(id: String, name: String, website: String, description: String)
case class Job(id: String, title: String, description: String, companyId: String, location: String, isRemote: Boolean)
case class Application(id: String, userId: String, jobId: String, status: String, coverLetter: Option[String])

// DATABASE SETUP (simplified for demonstration)
object DB {
  def transactorResource: Resource[IO, HikariTransactor[IO]] = for {
    ce <- ExecutionContexts.fixedThreadPool[IO](8)
    xa <- HikariTransactor.newHikariTransactor[IO](
      driverClassName = "org.postgresql.Driver",
      url = "jdbc:postgresql://localhost:5432/hiring",
      user = "postgres",
      pass = "password",
      connectEC = ce
    )
  } yield xa
}
// HTTP4s ROUTE
//object GraphQLRoute {
//  def apply[F[_]: Sync: Logger](schema: Schema[Dao[F], Unit], ctx: Dao[F]): HttpRoutes[F] = HttpRoutes.of[F] {
//    case req @ POST -> Root / "graphql" =>
//      for {
//        body <- req.as[String]
//        query <- IO.fromEither(QueryParser.parse(body))
//        result <- Executor.execute(schema, query, ctx)
//        response <- Ok(result.toString)
//      } yield response
//  }
//}

// MAIN
object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = ???
//    DB.transactorResource.use { xa =>
//      implicit val log = Slf4jLogger.getLogger[IO]
//      val userRepo = new DoobieUserRepo[IO](xa)
//      val context = ServiceContext(userRepo)
//
//      val httpApp = GraphQLRoute(GraphQLSchema.schema, context).orNotFound
//
//
//      BlazeServerBuilder[IO](global)
//        .bindHttp(8080, "0.0.0.0")
//        .withHttpApp(httpApp)
//        .resource
//        .use(_ => IO.never)
//        .as(ExitCode.Success)
//    }
}
