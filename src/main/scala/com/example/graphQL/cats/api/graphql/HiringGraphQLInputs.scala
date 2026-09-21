package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import sangria.marshalling.circe.*
import sangria.schema.*
import sangria.validation.ValueCoercionViolation

import java.time.Instant
import java.util.{Locale, UUID}
import scala.util.Try

private[graphql] object HiringGraphQLInputs {
  private final case class IdCoercionViolation(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value")
  private final case class InstantCoercionViolation()
      extends ValueCoercionViolation("Invalid Instant value; expected ISO-8601")
  private given Decoder[JobId] = Decoder.decodeUUID.map(JobId.apply)
  private given Decoder[ApplicationId] = Decoder.decodeUUID.map(ApplicationId.apply)
  private given Decoder[UserRole] = Decoder.decodeString.emapTry(value => Try(UserRole.valueOf(value)))
  private given Decoder[Instant] = Decoder.decodeString.emapTry(value => Try(Instant.parse(value)))

  given Decoder[JobFilterGraphQLInput] = deriveDecoder
  given Decoder[SubmitApplicationGraphQLInput] = deriveDecoder
  given Decoder[JobGraphQLInput] = deriveDecoder
  given Decoder[UpdateJobGraphQLInput] = deriveDecoder
  given Decoder[JobActionGraphQLInput] = deriveDecoder
  given Decoder[ApplicationActionGraphQLInput] = deriveDecoder
  given Decoder[RejectApplicationGraphQLInput] = deriveDecoder
  given Decoder[DeclineApplicationGraphQLInput] = deriveDecoder
  given Decoder[SignUpGraphQLInput] = deriveDecoder
  given Decoder[BootstrapAdminGraphQLInput] = deriveDecoder
  given Decoder[LoginGraphQLInput] = deriveDecoder
  given Decoder[UpdateProfileGraphQLInput] = deriveDecoder
  given Decoder[RecordJobViewGraphQLInput] = deriveDecoder
  given Decoder[RecordSearchResultClickGraphQLInput] = deriveDecoder

  lazy val healthStatus: EnumType[String] =
    EnumType("HealthStatus", values = List(EnumValue("UP", value = "UP")))
  lazy val readinessStatus: EnumType[String] =
    EnumType("ReadinessStatus", values = List(
      EnumValue("READY", value = "READY"), EnumValue("NOT_READY", value = "NOT_READY")))
  lazy val jobStatus: EnumType[JobStatus] =
    EnumType("JobStatus", values = JobStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  lazy val applicationStatus: EnumType[ApplicationStatus] =
    EnumType("ApplicationStatus", values = ApplicationStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  lazy val userRole: EnumType[UserRole] =
    EnumType("UserRole", values = UserRole.values.toList.map(role => EnumValue(role.toString, value = role)))
  lazy val userStatus: EnumType[AccountStatus] =
    EnumType("UserStatus", values = AccountStatus.values.toList.map(status => EnumValue(status.toString.toUpperCase(Locale.ROOT), value = status)))
  lazy val searchMode: EnumType[SearchMode] =
    EnumType("SearchMode", values = SearchMode.values.toList.map(mode => EnumValue(mode.toString, value = mode)))

  lazy val instantType: ScalarType[Instant] = ScalarType[Instant](
    "Instant",
    coerceUserInput = {
      case value: String => Either.catchNonFatal(Instant.parse(value)).left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    },
    coerceOutput = (value, _) => value.toString,
    coerceInput = {
      case sangria.ast.StringValue(value, _, _, _, _) =>
        Either.catchNonFatal(Instant.parse(value)).left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    }
  )

  def instantField[A](name: String, resolve: A => Instant): Field[RequestContext, A] =
    Field(name, instantType, resolve = context => resolve(context.value))

  private def idScalar[A](name: String, wrap: UUID => A, unwrap: A => UUID): ScalarType[A] =
    ScalarType[A](
      name,
      coerceUserInput = {
        case value: String => Either.catchNonFatal(UUID.fromString(value)).left.map(_ => IdCoercionViolation(name)).map(wrap)
        case _ => Left(IdCoercionViolation(name))
      },
      coerceOutput = (value, _) => unwrap(value).toString,
      coerceInput = {
        case sangria.ast.StringValue(value, _, _, _, _) =>
          Either.catchNonFatal(UUID.fromString(value)).left.map(_ => IdCoercionViolation(name)).map(wrap)
        case _ => Left(IdCoercionViolation(name))
      }
    )

  lazy val jobIdType: ScalarType[JobId] = idScalar("JobID", JobId.apply, _.value)
  lazy val applicationIdType: ScalarType[ApplicationId] = idScalar("ApplicationID", ApplicationId.apply, _.value)

  lazy val idArgument: Argument[JobId] = Argument("id", jobIdType)
  lazy val jobIdArgument: Argument[JobId] = Argument("jobId", jobIdType)
  lazy val queryArgument: Argument[String] = Argument("query", StringType)
  lazy val applicationIdArgument: Argument[ApplicationId] = Argument("applicationId", applicationIdType)
  lazy val firstArgument: Argument[Int] = Argument("first", IntType)
  lazy val afterArgument: Argument[Option[String]] = Argument("after", OptionInputType(StringType))
  lazy val cityArgument: Argument[Option[String]] = Argument("city", OptionInputType(StringType))
  lazy val skillsArgument: Argument[Option[Seq[String]]] = Argument("skills", OptionInputType(ListInputType(StringType)))
  lazy val createdAfterArgument: Argument[Option[Instant]] = Argument("createdAfter", OptionInputType(instantType))
  lazy val searchIdArgument: Argument[Option[String]] = Argument("searchId", OptionInputType(StringType))
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
      InputField("eventId", StringType), InputField("jobId", jobIdType), InputField("searchId", OptionInputType(StringType))))
  lazy val recordSearchResultClickInputType: InputObjectType[RecordSearchResultClickGraphQLInput] =
    InputObjectType[RecordSearchResultClickGraphQLInput]("RecordSearchResultClickInput", List(
      InputField("eventId", StringType), InputField("searchId", StringType), InputField("resultId", StringType)))
  lazy val recordJobViewInputArgument: Argument[RecordJobViewGraphQLInput] = Argument("input", recordJobViewInputType)
  lazy val recordSearchResultClickInputArgument: Argument[RecordSearchResultClickGraphQLInput] =
    Argument("input", recordSearchResultClickInputType)
}
