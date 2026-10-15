package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite

final class HiringGraphQLContractSpec extends CatsEffectSuite {
  private def fixture(name: String): IO[String] = IO.blocking {
    val stream = Option(getClass.getResourceAsStream(s"/graphql/$name"))
      .getOrElse(throw new IllegalArgumentException("Missing contract fixture"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }

  private def parseRequest(query: String): IO[GraphQLRequest] =
    IO.fromEither(
      Json
        .obj("query" -> Json.fromString(query))
        .as[GraphQLRequest]
        .left
        .map(error => new IllegalArgumentException("Invalid test operation", error))
    )

  private def executeRequest(
      request: GraphQLRequest,
      documentCache: GraphQLDocumentCache,
      context: Resource[IO, RequestContext]
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] =
    documentCache.document(request.query).flatMap {
      case Left(failure)   => IO.pure(Left(failure))
      case Right(document) => context.use(HiringGraphQLSchema.executeInContext(request, document, _))
    }

  test("interview scheduling operations match the active public schema") {
    fixture("interview-scheduling.graphql").map { operation =>
      val document = sangria.parser.QueryParser.parse(operation).get
      val violations =
        sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
      assertEquals(violations.toList, Nil)
    }
  }

  test(
    "DHW-31 interview cancellation operations match the active public schema, including every action's DomainError"
  ) {
    fixture("interview-cancellation.graphql").map { operation =>
      val document = sangria.parser.QueryParser.parse(operation).get
      val violations =
        sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
      assertEquals(violations.toList, Nil)
      val actions = List(
        "cancelInterview",
        "requestInterviewReschedule",
        "dismissInterviewRescheduleRequest",
        "proposeInterviewReschedule",
        "withdrawInterviewReschedule",
        "acceptInterviewReschedule",
        "declineInterviewReschedule"
      )
      val outcomes = document.operations.values.find(_.name.contains("InterviewActionOutcomes")).get
      val selected = outcomes.selections.collect { case field: sangria.ast.Field => field.name }
      assertEquals(selected.toList, actions)
      actions.foreach(action => assert(operation.contains(s"$action(input:"), action))
    }
  }

  test("embedding coverage operation matches the active public schema") {
    fixture("embedding-coverage.graphql").map { operation =>
      val document = sangria.parser.QueryParser.parse(operation).get
      val violations =
        sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
      assertEquals(violations.toList, Nil)
    }
  }

  test("ECR-07 the embedding coverage report exposes no identifiers, names, text, vectors or payloads") {
    val reportTypes = HiringGraphQLSchema.schema.allTypes.values.collect {
      case objectType: sangria.schema.ObjectType[?, ?] if objectType.name.startsWith("EmbeddingCoverage") => objectType
      case objectType: sangria.schema.ObjectType[?, ?] if objectType.name == "EmbeddingObservedModel"     => objectType
    }.toList
    val fieldNames = reportTypes.flatMap(_.fieldsByName.keys)
    assertEquals(reportTypes.size, 6)
    val forbidden = Set("id", "ids", "name", "text", "vector", "embedding", "payload", "entityId", "title")
    assertEquals(fieldNames.filter(forbidden.contains), Nil)
    assertEquals(
      HiringGraphQLSchema.schema.allTypes.values
        .collect { case enumType: sangria.schema.EnumType[?] => enumType }
        .filter(_.name.startsWith("Embedding"))
        .flatMap(_.values.map(_.name))
        .forall(label => label == label.toUpperCase),
      true
    )
  }

  test("DHW-31 served SDL matches the deterministic contract fixture") {
    fixture("hiring.graphql").map(expected => assertEquals(HiringGraphQLSchema.sdl, expected))
  }

  test("a closed request context causes a sanitized GraphQL field execution error") {
    for {
      parsed <- parseRequest("{ me { id } }")
      closed <- TestGraphQLSupport.context().use(IO.pure)
      result <- TestGraphQLSupport.parseAndExecute(parsed, closed)
    } yield {
      val body = result.fold(failure => fail(failure.toString), identity)
      val errors = body.hcursor.downField("errors").as[Vector[Json]]
      assertEquals(errors.map(_.size), Right(1))
      val error = errors.toOption.get.head
      assertEquals(error.hcursor.get[String]("message"), Right("Execution failed"))
      assertEquals(error.hcursor.get[Vector[String]]("path"), Right(Vector("me")))
      assert(error.hcursor.downField("locations").succeeded)
      assert(!body.noSpaces.contains("Request context is closed"))
      assert(!body.noSpaces.contains("IllegalStateException"))
    }
  }

  test("unexpected resolver failures are sanitized and reported after Sangria handles them") {
    for {
      reportCount <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics[IO] {
        def event(
            event: com.example.graphQL.cats.service.LogEvent,
            requestId: Option[String],
            fields: => Map[com.example.graphQL.cats.service.LogField, String]
        ) = reportCount.update(_ + 1)
      }
      request <- parseRequest("{ me { id } }")
      result <- TestGraphQLSupport
        .context(
          hiringReady = IO.raiseError[ProbeResult](new IllegalStateException("private resolver detail")),
          diagnostics = diagnostics
        )
        .use(context => TestGraphQLSupport.parseAndExecute(request, context))
      reports <- reportCount.get
    } yield {
      val body = result.fold(failure => fail(failure.toString), identity)
      assertEquals(body.hcursor.downField("errors").downArray.get[String]("message"), Right("Execution failed"))
      assert(!body.noSpaces.contains("private resolver detail"))
      assertEquals(reports, 1)
    }
  }

  test("malformed typed identifiers fail GraphQL coercion before resolver execution") {
    for {
      parsed <- parseRequest("{ job(id: \"not-a-uuid\") { id } }")
      result <- TestGraphQLSupport
        .dependencies()
        .use(dependencies =>
          executeRequest(
            parsed,
            dependencies.documentCache,
            dependencies.contextFactory.resource(
              RequestContextParameters(
                None,
                dependencies.hiring,
                dependencies.ensureHiringReady,
                diagnostics = Diagnostics.noop,
                requestId = Some("00000000-0000-0000-0000-000000000001")
              )
            )
          )
        )
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("query complexity rejection returns the typed invalid-query failure") {
    val fields = (1 to 510).map(index => s"me$index: me { id }").mkString(" ")
    for {
      request <- parseRequest(s"{ $fields }")
      result <- TestGraphQLSupport.dependencies().use { dependencies =>
        executeRequest(
          request,
          dependencies.documentCache,
          dependencies.contextFactory.resource(
            RequestContextParameters(
              None,
              dependencies.hiring,
              dependencies.ensureHiringReady,
              diagnostics = Diagnostics.noop
            )
          )
        )
      }
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("malformed UUID interaction inputs fail standard GraphQL validation") {
    val operations = List(
      "mutation { recordJobView(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"invalid\", jobId: \"00000000-0000-0000-0000-000000000001\" }) { recorded } }",
      "mutation { recordJobView(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"00000000-0000-0000-0000-000000000001\", jobId: \"00000000-0000-0000-0000-000000000001\", searchId: \"invalid\" }) { recorded } }",
      "mutation { recordSearchResultClick(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"invalid\", searchId: \"invalid\", resultId: \"invalid\" }) { recorded } }"
    )

    operations
      .traverse(parseRequest)
      .flatMap { requests =>
        TestGraphQLSupport.dependencies().use { dependencies =>
          requests.traverse { request =>
            executeRequest(
              request,
              dependencies.documentCache,
              dependencies.contextFactory.resource(
                RequestContextParameters(
                  None,
                  dependencies.hiring,
                  dependencies.ensureHiringReady,
                  diagnostics = Diagnostics.noop,
                  requestId = Some("00000000-0000-0000-0000-000000000001")
                )
              )
            )
          }
        }
      }
      .map(results => assert(results.forall(_ == Left(HiringGraphQLSchema.Failure.InvalidQuery))))
  }

  test("execute consumer fixture introspection.graphql") {
    for {
      query <- fixture("introspection.graphql")
      parsed <- parseRequest(query)
      result <- TestGraphQLSupport
        .dependencies()
        .use(dependencies =>
          executeRequest(
            parsed,
            dependencies.documentCache,
            dependencies.contextFactory.resource(
              RequestContextParameters(
                None,
                dependencies.hiring,
                dependencies.ensureHiringReady,
                diagnostics = Diagnostics.noop,
                requestId = Some("00000000-0000-0000-0000-000000000001"),
                schemaExposure = true
              )
            )
          )
        )
    } yield {
      val json = result.fold(failure => fail(failure.toString), identity)
      assert(json.hcursor.downField("data").succeeded)
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("AC-13.1 an executed fixture runs a hiring operation without any health or readiness field") {
    val receipt = "00000000-0000-0000-0000-0000000000a1"
    val actor =
      ActorContext(UserId(java.util.UUID.fromString("00000000-0000-0000-0000-0000000000b1")), UserRole.Candidate)
    for {
      query <- fixture("account-deletion-status.graphql")
      result <- TestGraphQLSupport
        .context(Some(actor))
        .use(context =>
          TestGraphQLSupport.parseAndExecute(
            GraphQLRequest(query, Json.obj("receiptId" -> Json.fromString(receipt)), None),
            context
          )
        )
    } yield {
      val json = result.fold(failure => fail(failure.toString), identity)
      assertEquals(json.hcursor.downField("data").get[String]("accountDeletionStatus"), Right("NOT_FOUND"))
      assert(!json.hcursor.downField("errors").succeeded, json.noSpaces)
    }
  }

  test("AC-13.1 the schema has no health or readiness field, type or enum") {
    val rootFields = HiringGraphQLSchema.schema.query.fieldsByName.keySet
    assert(!rootFields.contains("health") && !rootFields.contains("readiness"), rootFields.toString)
    val typeNames = HiringGraphQLSchema.schema.allTypes.keySet
    assertEquals(typeNames.filter(name => List("Health", "Readiness").exists(name.startsWith)), Set.empty[String])
    assert(!HiringGraphQLSchema.sdl.contains("HealthStatus") && !HiringGraphQLSchema.sdl.contains("ReadinessStatus"))
  }

  test("every operation fixture validates against the active schema") {
    val names = List(
      "account-deletion-status.graphql",
      "account-profile.graphql",
      "application-history.graphql",
      "embedding-coverage.graphql",
      "interview-cancellation.graphql",
      "interview-scheduling.graphql",
      "job-discovery-facets.graphql",
      "jobs.graphql"
    )
    names.traverse { name =>
      fixture(name).map { operation =>
        val document = sangria.parser.QueryParser.parse(operation).fold(error => fail(error.getMessage), identity)
        val violations =
          sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
        assertEquals(violations.map(_.errorMessage).toList, Nil, name)
      }
    }
  }

  // --- AC-01 descriptions ---------------------------------------------------------------------------------------

  test("AC-01.1 every root field, argument, input type and field, union and enum value has a description") {
    assertEquals(SchemaDescriptionCheck.undescribed(HiringGraphQLSchema.schema), Nil)
  }

  test("AC-01.2 a field added without a description is reported by name") {
    import sangria.schema.*
    val bare = Schema(
      ObjectType(
        "Query",
        fields[Unit, Unit](
          Field("described", StringType, description = Some("Documented."), resolve = _ => "x"),
          Field("bare", StringType, resolve = _ => "x")
        )
      )
    )
    assertEquals(SchemaDescriptionCheck.undescribed(bare), List("Query.bare"))
  }

  test("AC-01.3 login and signup descriptions state how they report refusals while the caller is unauthenticated") {
    val mutation = HiringGraphQLSchema.schema.mutation.get.fieldsByName
    List("login", "signUp").foreach { name =>
      val description = mutation(name).head.description.getOrElse("")
      assert(description.contains("not authenticated") && description.contains("top-level"), description)
    }
  }

  // --- AC-02 Long scalar ----------------------------------------------------------------------------------------

  test("AC-02.1 the SDL declares scalar Long and expectedRevision uses it") {
    assert(HiringGraphQLSchema.sdl.contains("\nscalar Long\n"))
    assert(HiringGraphQLSchema.sdl.contains("expectedRevision: Long!"))
    assert(!HiringGraphQLSchema.sdl.contains("expectedRevision: Int"))
  }

  private def coerceExpectedRevision(
      literal: String,
      variables: Json = Json.obj()
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] = {
    val id = "00000000-0000-0000-0000-000000000001"
    val mutation =
      s"""mutation Cancel($$revision: Long = $literal) {
         |  cancelInterview(input: { workflowId: "$id", expectedRevision: $$revision, idempotencyKey: "$id" }) { __typename }
         |}""".stripMargin
    TestGraphQLSupport
      .context(None)
      .use(context => TestGraphQLSupport.parseAndExecute(GraphQLRequest(mutation, variables, None), context))
  }

  test("AC-02.1 expectedRevision accepts a whole number as literal or variable") {
    List(
      coerceExpectedRevision("3"),
      coerceExpectedRevision("0", Json.obj("revision" -> Json.fromLong(9007199254740993L)))
    ).sequence.map(_.foreach(result => assertNotEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))))
  }

  test("AC-02.2 expectedRevision rejects fractions and strings in literals and variables") {
    List(
      coerceExpectedRevision("1.5"),
      coerceExpectedRevision("\"3\""),
      coerceExpectedRevision("0", Json.obj("revision" -> Json.fromDouble(1.5).get)),
      coerceExpectedRevision("0", Json.obj("revision" -> Json.fromString("3"))),
      coerceExpectedRevision("0", Json.obj("revision" -> Json.fromBigInt(BigInt(Long.MaxValue) + 1)))
    ).sequence.map(_.foreach(result => assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))))
  }

  // --- AC-03 error codes ----------------------------------------------------------------------------------------

  test("AC-03.0 every catalog error code is an ErrorCode value and every ErrorCode value is a catalog code") {
    val enumCodes = HiringGraphQLTypes.errorCodeType.values.map(_.name).toSet
    val catalogCodes = GraphQLFailureCatalog.errorCodes.map(_._1).toSet
    assertEquals(enumCodes, catalogCodes)
    assertEquals(HiringGraphQLTypes.errorCodeType.values.size, catalogCodes.size)
    assert(!enumCodes.contains("PERSISTED_QUERY_NOT_FOUND"))
    assertEquals(GraphQLFailureCatalog.codesWithConflictingDescriptions, Nil)
    val forbidden = HiringGraphQLTypes.errorCodeType.values.find(_.name == "FORBIDDEN").flatMap(_.description)
    assert(forbidden.exists(!_.contains("singleton")), forbidden.toString)
  }

  test("AC-03.1 all interview phases map to InterviewProgress values and both participants to InterviewParticipant") {
    import com.example.graphQL.cats.domain.workflow.{InterviewParticipant, InterviewWorkflowPhase}
    assertEquals(InterviewWorkflowPhase.values.length, 15)
    assertEquals(
      HiringGraphQLInputs.interviewProgressType.values.map(_.value).toList,
      InterviewWorkflowPhase.values.toList
    )
    assertEquals(
      HiringGraphQLInputs.interviewProgressType.values.map(_.name).toList,
      InterviewWorkflowPhase.values.toList.map(HiringGraphQLDsl.wireName)
    )
    assertEquals(
      HiringGraphQLInputs.interviewParticipantType.values.map(_.name).toList,
      List("CANDIDATE", "RECRUITER")
    )
    assertEquals(
      HiringGraphQLInputs.interviewParticipantType.values.map(_.value).toList,
      InterviewParticipant.values.toList
    )
  }

  test("AC-03.1 InterviewWorkflow.progress, notifiedParticipants and UserError.code are enums in the SDL") {
    val sdl = HiringGraphQLSchema.sdl
    assert(sdl.contains("progress: InterviewProgress!"))
    assert(sdl.contains("notifiedParticipants: [InterviewParticipant!]!"))
    assert(sdl.contains("code: ErrorCode!"))
    assert(sdl.contains("\nenum ErrorCode {"))
  }

  test("AC-05.5 the SDL declares the @oneOf directive it uses and still parses") {
    assert(HiringGraphQLSchema.sdl.startsWith("directive @oneOf on INPUT_OBJECT\n"))
    assertEquals(HiringGraphQLSchema.sdl.split("directive @oneOf", -1).length, 2)
    assert(sangria.parser.QueryParser.parse(HiringGraphQLSchema.sdl).isSuccess)
  }

  test("AC-05.0 the profile input is a oneOf input and rejects zero or two variants in a validated document") {
    assert(HiringGraphQLSchema.sdl.contains("input ProfileInput @oneOf {"))
    def validate(profile: String) = {
      val query =
        s"""mutation { updateMyProfile(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", profile: $profile }) { __typename } }"""
      val document = sangria.parser.QueryParser.parse(query).fold(error => fail(error.getMessage), identity)
      sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
    }
    assertEquals(validate("{ candidate: { skills: [\"Scala\"] } }").toList, Nil)
    assertEquals(validate("{ recruiter: { organizationName: \"Acme\" } }").toList, Nil)
    assert(validate("{ candidate: {}, recruiter: { organizationName: \"Acme\" } }").nonEmpty)
    assert(validate("{ }").nonEmpty)
    assert(validate("{ recruiter: {} }").nonEmpty, "organizationName is required")
  }

}
