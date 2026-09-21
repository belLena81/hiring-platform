package com.example.graphQL.cats.api.graphql

import cats.syntax.either.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import sangria.marshalling.circe.*
import sangria.schema.*
import sangria.validation.{ValueCoercionViolation, Violation}

import java.time.Instant
import java.util.{Locale, UUID}

private[graphql] object HiringGraphQLInputs {
  private final case class IdCoercionViolation(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value")
  private final case class NotAString(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value; expected a string")
  private final case class InstantCoercionViolation()
      extends ValueCoercionViolation("Invalid Instant value; expected ISO-8601")
  lazy val healthStatus: EnumType[String] = enumType("HealthStatus", List("UP"))
  lazy val readinessStatus: EnumType[String] = enumType("ReadinessStatus", List("READY", "NOT_READY"))
  lazy val jobStatus: EnumType[JobStatus] = enumType("JobStatus", JobStatus.values.toList)
  lazy val applicationStatus: EnumType[ApplicationStatus] = enumType("ApplicationStatus", ApplicationStatus.values.toList)
  lazy val userRole: EnumType[UserRole] = enumType("UserRole", UserRole.values.toList)
  lazy val userStatus: EnumType[AccountStatus] = enumType("UserStatus", AccountStatus.values.toList)
  lazy val searchMode: EnumType[SearchMode] = enumType("SearchMode", SearchMode.values.toList)

  private def enumType[A](name: String, values: List[A]): EnumType[A] =
    EnumType(name, values = values.map(value => EnumValue(value.toString.toUpperCase(Locale.ROOT), value = value)))

  private def stringScalar[A](name: String, parse: String => Either[Violation, A], render: A => String): ScalarType[A] =
    ScalarType[A](name,
      coerceOutput = (value, _) => render(value),
      coerceUserInput = { case value: String => parse(value); case _ => Left(NotAString(name)) },
      coerceInput = { case sangria.ast.StringValue(value, _, _, _, _) => parse(value); case _ => Left(NotAString(name)) })

  private def uuidScalar[A](name: String, wrap: UUID => A, unwrap: A => UUID): ScalarType[A] =
    stringScalar(name,
      value => Either.catchNonFatal(UUID.fromString(value)).left.map(_ => IdCoercionViolation(name)).map(wrap),
      value => unwrap(value).toString)

  lazy val instantType: ScalarType[Instant] =
    stringScalar("Instant", value => Either.catchNonFatal(Instant.parse(value)).left.map(_ => InstantCoercionViolation()), _.toString)

  def instantField[A](name: String, resolve: A => Instant): Field[RequestContext, A] =
    Field(name, instantType, resolve = context => resolve(context.value))

  lazy val uuidType: ScalarType[UUID] = uuidScalar[UUID]("UUID", value => value, value => value)
  lazy val userIdType: ScalarType[UserId] = uuidScalar("UserID", UserId.apply, _.value)
  lazy val jobIdType: ScalarType[JobId] = uuidScalar("JobID", JobId.apply, _.value)
  lazy val applicationIdType: ScalarType[ApplicationId] = uuidScalar("ApplicationID", ApplicationId.apply, _.value)

  lazy val idArgument: Argument[JobId] = Argument("id", jobIdType)
  lazy val jobIdArgument: Argument[JobId] = Argument("jobId", jobIdType)
  lazy val queryArgument: Argument[String] = Argument("query", StringType)
  lazy val applicationIdArgument: Argument[ApplicationId] = Argument("applicationId", applicationIdType)
  lazy val firstArgument: Argument[Int] = Argument("first", IntType)
  lazy val afterArgument: Argument[Option[String]] = Argument("after", OptionInputType(StringType))
  lazy val cityArgument: Argument[Option[String]] = Argument("city", OptionInputType(StringType))
  lazy val skillsArgument: Argument[Option[Seq[String]]] = Argument("skills", OptionInputType(ListInputType(StringType)))
  lazy val createdAfterArgument: Argument[Option[Instant]] = Argument("createdAfter", OptionInputType(instantType))
  lazy val searchIdArgument: Argument[Option[UUID]] = Argument("searchId", OptionInputType(uuidType))
  lazy val jobStatusArgument: Argument[Option[JobStatus]] = Argument("status", OptionInputType(jobStatus))
  lazy val applicationStatusArgument: Argument[Option[ApplicationStatus]] = Argument("status", OptionInputType(applicationStatus))
  lazy val userRoleArgument: Argument[Option[UserRole]] = Argument("role", OptionInputType(userRole))
  lazy val userStatusArgument: Argument[Option[AccountStatus]] = Argument("status", OptionInputType(userStatus))
  lazy val jobFilterInputType: InputObjectType[JobFilterGraphQLInput] = InputObjectType[JobFilterGraphQLInput]("JobFilter", List(
    InputField("city", OptionInputType(StringType)),
    InputField("skills", OptionInputType(ListInputType(StringType))),
    InputField("createdAfter", OptionInputType(instantType))
  ))
  lazy val jobFilterArgument: Argument[Option[JobFilterGraphQLInput]] = Argument("filter", OptionInputType(jobFilterInputType))
  lazy val submitApplicationInputType: InputObjectType[SubmitApplicationGraphQLInput] =
    InputObjectType[SubmitApplicationGraphQLInput]("SubmitApplicationInput", List(InputField("jobId", jobIdType)))
  lazy val jobInputType: InputObjectType[JobGraphQLInput] = InputObjectType[JobGraphQLInput]("JobInput", List(
    InputField("title", StringType),
    InputField("description", StringType),
    InputField("requirements", ListInputType(StringType)),
    InputField("skills", ListInputType(StringType)),
    InputField("country", StringType),
    InputField("city", OptionInputType(StringType)),
    InputField("remote", BooleanType)
  ))
  lazy val updateJobInputType: InputObjectType[UpdateJobGraphQLInput] = InputObjectType[UpdateJobGraphQLInput]("UpdateJobInput", List(
    InputField("id", jobIdType),
    InputField("patch", jobInputType)
  ))
  lazy val jobActionInputType: InputObjectType[JobActionGraphQLInput] =
    InputObjectType[JobActionGraphQLInput]("JobActionInput", List(InputField("jobId", jobIdType)))
  lazy val applicationActionInputType: InputObjectType[ApplicationActionGraphQLInput] =
    InputObjectType[ApplicationActionGraphQLInput]("ApplicationActionInput", List(InputField("applicationId", applicationIdType)))
  lazy val rejectApplicationInputType: InputObjectType[RejectApplicationGraphQLInput] = InputObjectType[RejectApplicationGraphQLInput](
    "RejectApplicationInput", List(InputField("applicationId", applicationIdType), InputField("feedback", OptionInputType(StringType))))
  lazy val declineApplicationInputType: InputObjectType[DeclineApplicationGraphQLInput] = InputObjectType[DeclineApplicationGraphQLInput](
    "DeclineApplicationInput", List(InputField("applicationId", applicationIdType), InputField("reason", OptionInputType(StringType))))
  lazy val submitApplicationInputArgument: Argument[SubmitApplicationGraphQLInput] = Argument("input", submitApplicationInputType)
  lazy val createJobInputArgument: Argument[JobGraphQLInput] = Argument("input", jobInputType)
  lazy val updateJobInputArgument: Argument[UpdateJobGraphQLInput] = Argument("input", updateJobInputType)
  lazy val jobActionInputArgument: Argument[JobActionGraphQLInput] = Argument("input", jobActionInputType)
  lazy val applicationActionInputArgument: Argument[ApplicationActionGraphQLInput] = Argument("input", applicationActionInputType)
  lazy val rejectApplicationInputArgument: Argument[RejectApplicationGraphQLInput] = Argument("input", rejectApplicationInputType)
  lazy val declineApplicationInputArgument: Argument[DeclineApplicationGraphQLInput] = Argument("input", declineApplicationInputType)
  lazy val signUpInputType: InputObjectType[SignUpGraphQLInput] = InputObjectType[SignUpGraphQLInput]("SignUpInput", List(
    InputField("name", StringType), InputField("role", userRole), InputField("password", StringType),
    InputField("skills", OptionInputType(ListInputType(StringType))), InputField("experienceSummary", OptionInputType(StringType)),
    InputField("resumeRef", OptionInputType(StringType)), InputField("organizationName", OptionInputType(StringType)),
    InputField("jobTitle", OptionInputType(StringType))))
  lazy val bootstrapAdminInputType: InputObjectType[BootstrapAdminGraphQLInput] =
    InputObjectType[BootstrapAdminGraphQLInput]("BootstrapAdminInput", List(InputField("name", StringType), InputField("password", StringType)))
  lazy val loginInputType: InputObjectType[LoginGraphQLInput] =
    InputObjectType[LoginGraphQLInput]("LoginInput", List(InputField("name", StringType), InputField("password", StringType)))
  lazy val updateProfileInputType: InputObjectType[UpdateProfileGraphQLInput] = InputObjectType[UpdateProfileGraphQLInput]("UpdateMyProfileInput", List(
    InputField("skills", OptionInputType(ListInputType(StringType))), InputField("experienceSummary", OptionInputType(StringType)),
    InputField("resumeRef", OptionInputType(StringType)), InputField("organizationName", OptionInputType(StringType)),
    InputField("jobTitle", OptionInputType(StringType))))
  lazy val signUpInputArgument: Argument[SignUpGraphQLInput] = Argument("input", signUpInputType)
  lazy val bootstrapAdminInputArgument: Argument[BootstrapAdminGraphQLInput] = Argument("input", bootstrapAdminInputType)
  lazy val loginInputArgument: Argument[LoginGraphQLInput] = Argument("input", loginInputType)
  lazy val updateProfileInputArgument: Argument[UpdateProfileGraphQLInput] = Argument("input", updateProfileInputType)
  lazy val recordJobViewInputType: InputObjectType[RecordJobViewGraphQLInput] =
    InputObjectType[RecordJobViewGraphQLInput]("RecordJobViewInput", List(
      InputField("eventId", uuidType), InputField("jobId", jobIdType), InputField("searchId", OptionInputType(uuidType))))
  lazy val recordSearchResultClickInputType: InputObjectType[RecordSearchResultClickGraphQLInput] =
    InputObjectType[RecordSearchResultClickGraphQLInput]("RecordSearchResultClickInput", List(
      InputField("eventId", uuidType), InputField("searchId", uuidType), InputField("resultId", uuidType)))
  lazy val recordJobViewInputArgument: Argument[RecordJobViewGraphQLInput] = Argument("input", recordJobViewInputType)
  lazy val recordSearchResultClickInputArgument: Argument[RecordSearchResultClickGraphQLInput] =
    Argument("input", recordSearchResultClickInputType)
}
