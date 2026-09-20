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
  private final class InvalidInput extends RuntimeException("Invalid GraphQL input", null, false, false)

  private type InputMap = Map[String, Any]

  private def inputAdapter[A](build: InputMap => Option[A]): FromInput[A] = new FromInput[A] {
    override val marshaller: CoercedScalaResultMarshaller = CoercedScalaResultMarshaller.default
    override def fromResult(node: marshaller.Node): A =
      inputMap(node).flatMap(build).getOrElse(invalidInput)
  }

  private def inputMap(value: Any): Option[InputMap] = value match {
    case fields: Map[?, ?] if fields.keys.forall(_.isInstanceOf[String]) =>
      Some(fields.iterator.collect { case (name: String, fieldValue) => name -> fieldValue }.toMap)
    case _ => None
  }

  private def invalidInput[A]: A =
    throw new InvalidInput

  private def requiredInput[A](fields: InputMap, name: String)(using extract: PartialFunction[Any, A]): Option[A] =
    fields.get(name).flatMap(extract.lift)

  private def optionalInput[A](fields: InputMap, name: String)(using extract: PartialFunction[Any, A]): Option[Option[A]] =
    // CoercedScalaResultMarshaller omits absent fields and stores present nullable fields as Some(value) or None.
    fields.get(name) match {
      case None | Some(None) => Some(None)
      case Some(Some(value)) => extract.lift(value).map(Some(_))
      case _ => None
    }

  private def requiredListInput[A](fields: InputMap, name: String)(using extract: PartialFunction[Any, A]): Option[List[A]] =
    fields.get(name).collect { case values: Seq[?] => values }
      .flatMap(values => values.foldRight(Option(List.empty[A]))((value, result) =>
        extract.lift(value).flatMap(element => result.map(element :: _))))

  private def optionalListInput[A](fields: InputMap, name: String)(using extract: PartialFunction[Any, A]): Option[Option[List[A]]] =
    fields.get(name) match {
      case None | Some(None) => Some(None)
      case Some(Some(values: Seq[?])) =>
        values.foldRight(Option(List.empty[A]))((value, result) =>
          extract.lift(value).flatMap(element => result.map(element :: _))).map(Some(_))
      case _ => None
    }

  private given stringInput: PartialFunction[Any, String] = { case value: String => value }
  private given booleanInput: PartialFunction[Any, Boolean] = { case value: Boolean => value }
  private given jobIdInput: PartialFunction[Any, JobId] = { case value: UUID => JobId(value) }
  private given applicationIdInput: PartialFunction[Any, ApplicationId] = { case value: UUID => ApplicationId(value) }
  private given userRoleInput: PartialFunction[Any, UserRole] = { case value: UserRole => value }
  private given instantInput: PartialFunction[Any, Instant] = { case value: Instant => value }

  private def jobGraphQLInput(fields: InputMap): Option[JobGraphQLInput] =
    for {
      title <- requiredInput[String](fields, "title")
      description <- requiredInput[String](fields, "description")
      requirements <- requiredListInput[String](fields, "requirements")
      skills <- requiredListInput[String](fields, "skills")
      country <- requiredInput[String](fields, "country")
      city <- optionalInput[String](fields, "city")
      remote <- requiredInput[Boolean](fields, "remote")
    } yield JobGraphQLInput(title, description, requirements, skills, country, city, remote)

  private def jobGraphQLInput(value: Any): Option[JobGraphQLInput] =
    value match {
      case input: JobGraphQLInput => Some(input)
      case fields: Map[?, ?] => inputMap(fields).flatMap(jobGraphQLInput)
      case _ => None
    }

  given FromInput[JobFilterGraphQLInput] = inputAdapter(fields =>
    for {
      city <- optionalInput[String](fields, "city")
      skills <- optionalListInput[String](fields, "skills")
      createdAfter <- optionalInput[Instant](fields, "createdAfter")
    } yield JobFilterGraphQLInput(city, skills, createdAfter))
  given FromInput[SubmitApplicationGraphQLInput] = inputAdapter(fields =>
    requiredInput[JobId](fields, "jobId").map(SubmitApplicationGraphQLInput.apply))
  given FromInput[JobGraphQLInput] = inputAdapter(jobGraphQLInput)
  given FromInput[UpdateJobGraphQLInput] = inputAdapter(fields =>
    for {
      id <- requiredInput[JobId](fields, "id")
      patchValue <- fields.get("patch")
      patch <- jobGraphQLInput(patchValue)
    } yield UpdateJobGraphQLInput(id, patch))
  given FromInput[JobActionGraphQLInput] = inputAdapter(fields =>
    requiredInput[JobId](fields, "jobId").map(JobActionGraphQLInput.apply))
  given FromInput[ApplicationActionGraphQLInput] = inputAdapter(fields =>
    requiredInput[ApplicationId](fields, "applicationId").map(ApplicationActionGraphQLInput.apply))
  given FromInput[RejectApplicationGraphQLInput] = inputAdapter(fields =>
    for {
      applicationId <- requiredInput[ApplicationId](fields, "applicationId")
      feedback <- optionalInput[String](fields, "feedback")
    } yield RejectApplicationGraphQLInput(applicationId, feedback))
  given FromInput[DeclineApplicationGraphQLInput] = inputAdapter(fields =>
    for {
      applicationId <- requiredInput[ApplicationId](fields, "applicationId")
      reason <- optionalInput[String](fields, "reason")
    } yield DeclineApplicationGraphQLInput(applicationId, reason))
  given FromInput[SignUpGraphQLInput] = inputAdapter(fields =>
    for {
      name <- requiredInput[String](fields, "name")
      role <- requiredInput[UserRole](fields, "role")
      password <- requiredInput[String](fields, "password")
      skills <- optionalListInput[String](fields, "skills")
      experienceSummary <- optionalInput[String](fields, "experienceSummary")
      resumeRef <- optionalInput[String](fields, "resumeRef")
      organizationName <- optionalInput[String](fields, "organizationName")
      jobTitle <- optionalInput[String](fields, "jobTitle")
    } yield SignUpGraphQLInput(name, role, password, skills, experienceSummary, resumeRef, organizationName, jobTitle))
  given FromInput[BootstrapAdminGraphQLInput] = inputAdapter(fields =>
    for {
      name <- requiredInput[String](fields, "name")
      password <- requiredInput[String](fields, "password")
    } yield BootstrapAdminGraphQLInput(name, password))
  given FromInput[LoginGraphQLInput] = inputAdapter(fields =>
    for {
      name <- requiredInput[String](fields, "name")
      password <- requiredInput[String](fields, "password")
    } yield LoginGraphQLInput(name, password))
  given FromInput[UpdateProfileGraphQLInput] = inputAdapter(fields =>
    for {
      skills <- optionalListInput[String](fields, "skills")
      experienceSummary <- optionalInput[String](fields, "experienceSummary")
      resumeRef <- optionalInput[String](fields, "resumeRef")
      organizationName <- optionalInput[String](fields, "organizationName")
      jobTitle <- optionalInput[String](fields, "jobTitle")
    } yield UpdateProfileGraphQLInput(skills, experienceSummary, resumeRef, organizationName, jobTitle))
  given FromInput[RecordJobViewGraphQLInput] = inputAdapter(fields =>
    for {
      eventId <- requiredInput[String](fields, "eventId")
      jobId <- requiredInput[JobId](fields, "jobId")
      searchId <- optionalInput[String](fields, "searchId")
    } yield RecordJobViewGraphQLInput(eventId, jobId, searchId))
  given FromInput[RecordSearchResultClickGraphQLInput] = inputAdapter(fields =>
    for {
      eventId <- requiredInput[String](fields, "eventId")
      searchId <- requiredInput[String](fields, "searchId")
      resultId <- requiredInput[String](fields, "resultId")
    } yield RecordSearchResultClickGraphQLInput(eventId, searchId, resultId))

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
