package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
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

  private type InputMap = Map[String, Any]

  private def inputAdapter[A](build: InputMap => A): FromInput[A] = new FromInput[A] {
    override val marshaller: CoercedScalaResultMarshaller = CoercedScalaResultMarshaller.default
    override def fromResult(node: marshaller.Node): A =
      build(node.asInstanceOf[InputMap])
  }

  private def requiredInput[A](fields: InputMap, name: String): A =
    fields(name).asInstanceOf[A]

  private def optionalInput[A](fields: InputMap, name: String): Option[A] =
    fields.get(name).flatMap {
      case None => None
      case Some(value) => Some(value.asInstanceOf[A])
      case value => Some(value.asInstanceOf[A])
    }

  private def requiredListInput[A](fields: InputMap, name: String): List[A] =
    requiredInput[Seq[A]](fields, name).toList

  private def optionalListInput[A](fields: InputMap, name: String): Option[List[A]] =
    optionalInput[Seq[A]](fields, name).map(_.toList)

  private def jobGraphQLInput(fields: InputMap): JobGraphQLInput =
    JobGraphQLInput(
      requiredInput[String](fields, "title"),
      requiredInput[String](fields, "description"),
      requiredListInput[String](fields, "requirements"),
      requiredListInput[String](fields, "skills"),
      requiredInput[String](fields, "country"),
      optionalInput[String](fields, "city"),
      requiredInput[Boolean](fields, "remote")
    )

  private def jobGraphQLInput(value: Any): JobGraphQLInput =
    value match {
      case input: JobGraphQLInput => input
      case fields: Map[?, ?] => jobGraphQLInput(fields.asInstanceOf[InputMap])
    }

  given FromInput[JobFilterGraphQLInput] = inputAdapter(fields =>
    JobFilterGraphQLInput(optionalInput[String](fields, "city"), optionalListInput[String](fields, "skills"),
      optionalInput[Instant](fields, "createdAfter")))
  given FromInput[SubmitApplicationGraphQLInput] = inputAdapter(fields =>
    SubmitApplicationGraphQLInput(requiredInput[JobId](fields, "jobId")))
  given FromInput[JobGraphQLInput] = inputAdapter(jobGraphQLInput)
  given FromInput[UpdateJobGraphQLInput] = inputAdapter(fields =>
    UpdateJobGraphQLInput(requiredInput[JobId](fields, "id"), jobGraphQLInput(requiredInput[Any](fields, "patch"))))
  given FromInput[JobActionGraphQLInput] = inputAdapter(fields =>
    JobActionGraphQLInput(requiredInput[JobId](fields, "jobId")))
  given FromInput[ApplicationActionGraphQLInput] = inputAdapter(fields =>
    ApplicationActionGraphQLInput(requiredInput[ApplicationId](fields, "applicationId")))
  given FromInput[RejectApplicationGraphQLInput] = inputAdapter(fields =>
    RejectApplicationGraphQLInput(requiredInput[ApplicationId](fields, "applicationId"), optionalInput[String](fields, "feedback")))
  given FromInput[DeclineApplicationGraphQLInput] = inputAdapter(fields =>
    DeclineApplicationGraphQLInput(requiredInput[ApplicationId](fields, "applicationId"), optionalInput[String](fields, "reason")))
  given FromInput[SignUpGraphQLInput] = inputAdapter(fields =>
    SignUpGraphQLInput(requiredInput[String](fields, "name"), requiredInput[UserRole](fields, "role"),
      requiredInput[String](fields, "password"), optionalListInput[String](fields, "skills"),
      optionalInput[String](fields, "experienceSummary"), optionalInput[String](fields, "resumeRef"),
      optionalInput[String](fields, "organizationName"), optionalInput[String](fields, "jobTitle")))
  given FromInput[BootstrapAdminGraphQLInput] = inputAdapter(fields =>
    BootstrapAdminGraphQLInput(requiredInput[String](fields, "name"), requiredInput[String](fields, "password")))
  given FromInput[LoginGraphQLInput] = inputAdapter(fields =>
    LoginGraphQLInput(requiredInput[String](fields, "name"), requiredInput[String](fields, "password")))
  given FromInput[UpdateProfileGraphQLInput] = inputAdapter(fields =>
    UpdateProfileGraphQLInput(optionalListInput[String](fields, "skills"), optionalInput[String](fields, "experienceSummary"),
      optionalInput[String](fields, "resumeRef"), optionalInput[String](fields, "organizationName"),
      optionalInput[String](fields, "jobTitle")))

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
      case value: String => Try(Instant.parse(value)).toEither.left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    },
    coerceOutput = (value, _) => value.toString,
    coerceInput = {
      case sangria.ast.StringValue(value, _, _, _, _) =>
        Try(Instant.parse(value)).toEither.left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    }
  )

  def instantField[A](name: String, resolve: A => Instant): Field[RequestContext, A] =
    Field(name, instantType, resolve = context => resolve(context.value))

  private def idScalar[A](name: String, wrap: UUID => A, unwrap: A => UUID): ScalarType[A] =
    ScalarType[A](
      name,
      coerceUserInput = {
        case value: String => Try(UUID.fromString(value)).toEither.left.map(_ => IdCoercionViolation(name)).map(wrap)
        case _ => Left(IdCoercionViolation(name))
      },
      coerceOutput = (value, _) => unwrap(value).toString,
      coerceInput = {
        case sangria.ast.StringValue(value, _, _, _, _) =>
          Try(UUID.fromString(value)).toEither.left.map(_ => IdCoercionViolation(name)).map(wrap)
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
}
