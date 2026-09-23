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

  private val probeInputType = InputObjectType[ProbeInput](
    "ProbeInput",
    List(
      InputField("required", StringType),
      InputField("optional", OptionInputType(StringType))
    )
  )
  private val probeInputArgument = Argument[ProbeInput]("input", probeInputType)
  private val probeOutputType = ObjectType(
    "ProbeOutput",
    fields[Unit, ProbeInput](
      Field("required", StringType, resolve = _.value.fields("required").asInstanceOf[String]),
      Field(
        "optionalShape",
        StringType,
        resolve = context =>
          context.value.fields.get("optional") match {
            case Some(Some(_: String)) => "PRESENT"
            case Some(None)            => "NONE"
            case None                  => "MISSING"
            case _                     => "UNEXPECTED"
          }
      ),
      Field(
        "optional",
        OptionType(StringType),
        resolve = _.value.fields.get("optional").collect { case Some(value: String) =>
          value
        }
      )
    )
  )
  private val schema = Schema(
    ObjectType("Query", fields[Unit, Unit](Field("health", StringType, resolve = _ => "UP"))),
    Some(
      ObjectType(
        "Mutation",
        fields[Unit, Unit](
          Field(
            "inspect",
            probeOutputType,
            arguments = probeInputArgument :: Nil,
            resolve = (context: Context[Unit, Unit]) => Value(context.arg(probeInputArgument))
          )
        )
      )
    )
  )
  private val mutation =
    """mutation Inspect($input: ProbeInput!) {
      |  inspect(input: $input) { required optionalShape optional }
      |}""".stripMargin

  private val updateInputArgument = Argument("input", HiringGraphQLInputs.updateJobInputType)
  private val updateSchema = Schema(
    ObjectType("UpdateQuery", fields[Unit, Unit](Field("health", StringType, resolve = _ => "UP"))),
    Some(
      ObjectType(
        "UpdateMutation",
        fields[Unit, Unit](
          Field(
            "inspectUpdate",
            StringType,
            arguments = updateInputArgument :: Nil,
            resolve = context => context.arg(updateInputArgument).patch.title
          )
        )
      )
    )
  )
  private val updateMutation =
    """mutation InspectUpdate($input: UpdateJobInput!) {
      |  inspectUpdate(input: $input)
      |}""".stripMargin

  test("CoercedScalaResultMarshaller omits absent optional variables and wraps present nullable values") {
    val present = Json.obj(
      "input" -> Json.obj(
        "required" -> Json.fromString("required-present"),
        "optional" -> Json.fromString("optional-present")
      )
    )
    val absent = Json.obj("input" -> Json.obj("required" -> Json.fromString("required-absent")))
    val explicitNull = Json.obj(
      "input" -> Json.obj(
        "required" -> Json.fromString("required-null"),
        "optional" -> Json.Null
      )
    )

    (execute(present), execute(absent), execute(explicitNull)).mapN { (presentResult, absentResult, nullResult) =>
      assertProbe(presentResult, "required-present", "PRESENT", Some("optional-present"))
      assertProbe(absentResult, "required-absent", "MISSING", None)
      assertProbe(nullResult, "required-null", "NONE", None)
    }
  }

  test("null or scalar nested job patches are rejected without exposing conversion failures") {
    val id = "00000000-0000-0000-0000-000000000001"
    val nullPatch = Json.obj("input" -> Json.obj("id" -> Json.fromString(id), "patch" -> Json.Null))
    val scalarPatch = Json.obj("input" -> Json.obj("id" -> Json.fromString(id), "patch" -> Json.fromString("invalid")))

    (executeUpdate(nullPatch), executeUpdate(scalarPatch)).mapN { (nullResult, scalarResult) =>
      List(nullResult, scalarResult).foreach { result =>
        assert(result.isLeft)
        val message = result.fold(_.getMessage, _.noSpaces)
        assert(!message.contains("ClassCastException"))
        assert(!message.contains("MatchError"))
      }
    }
  }

  test("malformed JSON input fails through the Circe decoder") {
    val malformedInputs = List(
      Json.obj(),
      Json.obj("title" -> Json.fromInt(1)),
      Json.obj(
        "title" -> Json.fromString("title"),
        "description" -> Json.fromString("description"),
        "requirements" -> Json.fromString("not-a-list"),
        "skills" -> Json.arr(Json.fromString("Scala")),
        "country" -> Json.fromString("Cyprus"),
        "remote" -> Json.fromBoolean(true)
      ),
      Json.obj(
        "title" -> Json.fromString("title"),
        "description" -> Json.fromString("description"),
        "requirements" -> Json.arr(Json.fromString("requirement")),
        "skills" -> Json.arr(Json.fromString("Scala")),
        "country" -> Json.fromString("Cyprus"),
        "remote" -> Json.fromString("not-a-boolean")
      )
    )

    malformedInputs.foreach { input =>
      val result = summon[io.circe.Decoder[HiringGraphQLModel.JobGraphQLInput]].decodeJson(input)
      assert(result.isLeft)
      assert(result.left.exists(_.getMessage.nonEmpty))
    }
  }

  private def execute(variables: Json): IO[Json] =
    IO.fromEither(QueryParser.parse(mutation).toEither).flatMap { query =>
      IO.executionContext.flatMap { implicit executionContext =>
        IO.fromFuture(IO(Executor.execute(schema, query, variables = variables)))
      }
    }

  private def executeUpdate(variables: Json): IO[Either[Throwable, Json]] =
    IO.fromEither(QueryParser.parse(updateMutation).toEither).flatMap { query =>
      IO.executionContext.flatMap { implicit executionContext =>
        IO.fromFuture(IO(Executor.execute(updateSchema, query, variables = variables))).attempt
      }
    }

  private def assertProbe(result: Json, required: String, optionalShape: String, optional: Option[String]): Unit = {
    val payload = result.hcursor.downField("data").downField("inspect")
    assertEquals(payload.get[String]("required"), Right(required))
    assertEquals(payload.get[String]("optionalShape"), Right(optionalShape))
    assertEquals(payload.get[Option[String]]("optional"), Right(optional))
  }
}
