package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.GraphQLFailure
import com.example.graphQL.cats.shared.CauseChain
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.control.NoStackTrace
import java.util.concurrent.ConcurrentLinkedQueue
import sangria.execution.{ExceptionHandler, HandledException, QueryAnalysisError}
import sangria.marshalling.ResultMarshaller

/** Owns the effect and Future interop required by Sangria's execution API. */
private[graphql] final class HiringGraphQLSangriaAdapter(dispatcher: Dispatcher[IO]) {
  def resolverFuture[A](context: RequestContext, action: HiringGraphQLResult[A]): Future[A] =
    dispatcher.unsafeToFuture(
      context.requestScoped(action).value.flatMap {
        case Right(value)  => IO.pure(value)
        case Left(failure) => IO.raiseError(ResolverFailureSignal(failure))
      }
    )

  def fieldFuture[A](context: RequestContext, name: String, action: HiringGraphQLResult[A]): Future[A] =
    resolverFuture(context, context.traceField(name, action))

  def fromFuture[A](future: IO[Future[A]]): IO[A] = IO.fromFuture(future)

  // Sangria requires an ExecutionContext for deferred-value projections; keep them on the calling thread.
  def deferredExecutionContext: ExecutionContext = ExecutionContext.parasitic

  def execute[A](context: RequestContext)(run: ExceptionHandler => IO[A]): IO[Either[HiringGraphQLSchema.Failure, A]] =
    IO.defer {
      val unexpectedFailures = new ConcurrentLinkedQueue[Throwable]()
      run(exceptionHandler(unexpectedFailures)).attempt.flatMap {
        case Right(value) =>
          report(context, unexpectedFailures).as(Right(value))
        case Left(error) if isInvalidQuery(error) => IO.pure(Left(HiringGraphQLSchema.Failure.InvalidQuery))
        case Left(error)                          =>
          (context.reportExecutionFailure(error) *> report(context, unexpectedFailures))
            .as(Left(HiringGraphQLSchema.Failure.Internal))
      }
    }

  /** Sangria's reducer requires a Throwable; keep this framework signal private to the adapter. */
  def complexityRejected(limit: Double): Throwable = ComplexityRejected(limit)

  private def exceptionHandler(unexpectedFailures: ConcurrentLinkedQueue[Throwable]): ExceptionHandler =
    ExceptionHandler {
      case (marshaller, ResolverFailureSignal(failure)) => handledFailure(marshaller, failure)
      case (_, error)                                   =>
        unexpectedFailures.add(error)
        HandledException("Execution failed")
    }

  private def handledFailure(
      marshaller: ResultMarshaller,
      failure: HiringGraphQLFailure
  ): HandledException =
    failure match {
      case HiringGraphQLFailure.UseCase(error) =>
        handledGraphQLFailure(marshaller, HiringGraphQLResolverSupport.toGraphQLFailure(error))
      case HiringGraphQLFailure.Input(error)         => handledGraphQLFailure(marshaller, error)
      case HiringGraphQLFailure.RateLimited(seconds) =>
        HandledException(
          "Too many authentication attempts",
          Map(
            "code" -> marshaller.scalarNode("RATE_LIMITED", "String", Set.empty),
            "retryAfter" -> marshaller.scalarNode(seconds, "Int", Set.empty)
          )
        )
      case HiringGraphQLFailure.RequestClosed => HandledException("Execution failed")
    }

  private def handledGraphQLFailure(marshaller: ResultMarshaller, failure: GraphQLFailure): HandledException =
    HandledException(failure.message, Map("code" -> marshaller.scalarNode(failure.code, "String", Set.empty)))

  private def report(context: RequestContext, failures: ConcurrentLinkedQueue[Throwable]): IO[Unit] =
    failures.iterator().asScala.toList.traverse_(context.reportExecutionFailure)

  private def isInvalidQuery(error: Throwable): Boolean =
    error match {
      case _: QueryAnalysisError => true
      case other                 => CauseChain(other).exists(_.isInstanceOf[ComplexityRejected])
    }
}

private final case class ResolverFailureSignal(failure: HiringGraphQLFailure) extends RuntimeException with NoStackTrace

private final case class ComplexityRejected(limit: Double)
    extends RuntimeException(s"Query complexity exceeds $limit")
    with NoStackTrace
