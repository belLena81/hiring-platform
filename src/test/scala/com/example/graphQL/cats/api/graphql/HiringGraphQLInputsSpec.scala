package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import munit.CatsEffectSuite
import sangria.execution.Executor
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
import sangria.marshalling.circe.*
import sangria.parser.QueryParser
import sangria.schema.*

final class HiringGraphQLInputsSpec extends CatsEffectSuite {
  private final case class ProbeInput(fields: Map[String, Any])

  private given FromInput[ProbeInput] = new FromInput[ProbeInput] {
    override val marshaller: CoercedScalaResultMarshaller = CoercedScalaResultMarshaller.default
    override def fromResult(node: marshaller.Node): ProbeInput = ProbeInput(node.asInstanceOf[Map[String, Any]])
  }

  private val probeInputType = InputObjectType[ProbeInput]("ProbeInput", List(
    InputField("required", StringType),
    InputField("optional", OptionInputType(StringType))
  ))
  private val probeInputArgument = Argument[ProbeInput]("input", probeInputType)
  private val probeOutputType = ObjectType("ProbeOutput", fields[Unit, ProbeInput](
    Field("required", StringType, resolve = _.value.fields("required").asInstanceOf[String]),
    Field("optionalShape", StringType, resolve = context => context.value.fields.get("optional") match {
      case Some(Some(_: String)) => "PRESENT"
      case Some(None) => "NONE"
      case None => "MISSING"
      case _ => "UNEXPECTED"
    }),
    Field("optional", OptionType(StringType), resolve = _.value.fields.get("optional").collect {
      case Some(value: String) => value
    })
  ))
  private val schema = Schema(
    ObjectType("Query", fields[Unit, Unit](Field("health", StringType, resolve = _ => "UP"))),
    Some(ObjectType("Mutation", fields[Unit, Unit](
      Field("inspect", probeOutputType, arguments = probeInputArgument :: Nil,
        resolve = (context: Context[Unit, Unit]) => Value(context.arg(probeInputArgument)))
    )))
  )
  private val mutation =
    """mutation Inspect($input: ProbeInput!) {
      |  inspect(input: $input) { required optionalShape optional }
      |}""".stripMargin

  test("CoercedScalaResultMarshaller omits absent optional variables and wraps present nullable values") {
    val present = Json.obj("input" -> Json.obj(
      "required" -> Json.fromString("required-present"),
      "optional" -> Json.fromString("optional-present")
    ))
    val absent = Json.obj("input" -> Json.obj("required" -> Json.fromString("required-absent")))
    val explicitNull = Json.obj("input" -> Json.obj(
      "required" -> Json.fromString("required-null"),
      "optional" -> Json.Null
    ))

    (execute(present), execute(absent), execute(explicitNull)).mapN { (presentResult, absentResult, nullResult) =>
      assertProbe(presentResult, "required-present", "PRESENT", Some("optional-present"))
      assertProbe(absentResult, "required-absent", "MISSING", None)
      assertProbe(nullResult, "required-null", "NONE", None)
    }
  }

  private def execute(variables: Json): IO[Json] =
    IO.fromEither(QueryParser.parse(mutation).toEither).flatMap { query =>
      IO.executionContext.flatMap { implicit executionContext =>
        IO.fromFuture(IO(Executor.execute(schema, query, variables = variables)))
      }
    }

  private def assertProbe(result: Json, required: String, optionalShape: String, optional: Option[String]): Unit = {
    val payload = result.hcursor.downField("data").downField("inspect")
    assertEquals(payload.get[String]("required"), Right(required))
    assertEquals(payload.get[String]("optionalShape"), Right(optionalShape))
    assertEquals(payload.get[Option[String]]("optional"), Right(optional))
  }
}
