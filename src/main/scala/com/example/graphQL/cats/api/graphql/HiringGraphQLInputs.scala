package com.example.graphQL.cats.api.graphql

import cats.syntax.either.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.{describedEnum, describedNames}
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.Identifiers.{
  ApplicationEventId,
  ApplicationId,
  JobId,
  UserId,
  parse as parseIdentifier
}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.workflow.{InterviewParticipant, InterviewWorkflowId, InterviewWorkflowPhase}
import sangria.macros.derive.*
import sangria.marshalling.circe.*
import sangria.schema.*
import sangria.validation.{ValueCoercionViolation, Violation}

import java.time.Instant
import java.util.UUID
import com.example.graphQL.cats.shared.Parsing

private[graphql] object HiringGraphQLInputs {
  private final case class IdCoercionViolation(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value")
  private final case class NotAString(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value; expected a string")
  private final case class InstantCoercionViolation()
      extends ValueCoercionViolation("Invalid Instant value; expected ISO-8601")
  private final case class LongCoercionViolation()
      extends ValueCoercionViolation("Invalid Long value; expected an integer in the signed 64-bit range")

  // --- Scalars ----------------------------------------------------------------------------------------------------

  private def stringScalar[A](
      name: String,
      description: String,
      parse: String => Either[Violation, A],
      render: A => String
  ): ScalarType[A] =
    ScalarType[A](
      name,
      description = Some(description),
      coerceOutput = (value, _) => render(value),
      coerceUserInput = { case value: String => parse(value); case _ => Left(NotAString(name)) },
      coerceInput = {
        case sangria.ast.StringValue(value, _, _, _, _) => parse(value); case _ => Left(NotAString(name))
      }
    )

  private def uuidScalar[A](
      name: String,
      description: String,
      parse: String => Option[A],
      unwrap: A => UUID
  ): ScalarType[A] =
    stringScalar(
      name,
      description,
      value => parse(value).toRight(IdCoercionViolation(name)),
      value => unwrap(value).toString
    )

  private val SafeIntegerBound = 9007199254740992d

  /** A 64-bit integer, serialized as a JSON number. Non-integral, non-numeric and out-of-range input is rejected. */
  lazy val longType: ScalarType[Long] =
    ScalarType[Long](
      "Long",
      description = Some("A signed 64-bit integer, serialized as a JSON number. Fractions and strings are rejected."),
      coerceOutput = (value, _) => value,
      coerceUserInput = {
        case value: Int                                                                        => Right(value.toLong)
        case value: Long                                                                       => Right(value)
        case value: BigInt if value.isValidLong                                                => Right(value.longValue)
        case value: BigDecimal if value.isValidLong                                            => Right(value.longValue)
        case value: Double if value == math.rint(value) && math.abs(value) <= SafeIntegerBound => Right(value.toLong)
        case _ => Left(LongCoercionViolation())
      },
      coerceInput = {
        case sangria.ast.IntValue(value, _, _)                         => Right(value.toLong)
        case sangria.ast.BigIntValue(value, _, _) if value.isValidLong => Right(value.longValue)
        case _                                                         => Left(LongCoercionViolation())
      }
    )

  lazy val instantType: ScalarType[Instant] =
    stringScalar(
      "Instant",
      "A UTC instant in ISO-8601 format, for example 2026-01-31T09:30:00Z.",
      value => Either.catchNonFatal(Instant.parse(value)).left.map(_ => InstantCoercionViolation()),
      _.toString
    )

  def instantField[A](name: String, resolve: A => Instant): Field[RequestContext, A] =
    Field(name, instantType, resolve = context => resolve(context.value))

  lazy val uuidType: ScalarType[UUID] =
    uuidScalar[UUID](
      "UUID",
      "A UUID in its canonical textual form.",
      value => Parsing.parseUuid(value).toOption,
      value => value
    )
  lazy val userIdType: ScalarType[UserId] =
    uuidScalar("UserID", "The identifier of a user.", value => parseIdentifier(value)(UserId.apply), _.value)
  lazy val jobIdType: ScalarType[JobId] =
    uuidScalar("JobID", "The identifier of a job.", value => parseIdentifier(value)(JobId.apply), _.value)
  lazy val applicationIdType: ScalarType[ApplicationId] =
    uuidScalar(
      "ApplicationID",
      "The identifier of an application.",
      value => parseIdentifier(value)(ApplicationId.apply),
      _.value
    )
  lazy val applicationEventIdType: ScalarType[ApplicationEventId] =
    uuidScalar(
      "ApplicationEventID",
      "The identifier of an entry in an application's history.",
      value => parseIdentifier(value)(ApplicationEventId.apply),
      _.value
    )
  lazy val interviewWorkflowIdType: ScalarType[InterviewWorkflowId] =
    uuidScalar(
      "InterviewWorkflowID",
      "The identifier of an interview workflow.",
      value => Parsing.parseUuid(value).toOption.map(InterviewWorkflowId.apply),
      _.value
    )
  lazy val searchIdType: ScalarType[SearchId] =
    uuidScalar(
      "SearchID",
      "The identifier of one search, issued by the server or supplied by the client to correlate later interactions.",
      value => Parsing.parseUuid(value).toOption.map(SearchId.apply),
      _.value
    )
  lazy val deletionReceiptIdType: ScalarType[String] =
    stringScalar(
      "DeletionReceiptID",
      "The receipt identifier returned when an account deletion was requested.",
      value => Parsing.parseUuid(value).toOption.map(_.toString).toRight(IdCoercionViolation("DeletionReceiptID")),
      identity
    )

  given ScalarType[Instant] = instantType
  given ScalarType[UUID] = uuidType
  given ScalarType[Long] = longType
  given ScalarType[UserId] = userIdType
  given ScalarType[JobId] = jobIdType
  given ScalarType[ApplicationId] = applicationIdType
  given ScalarType[InterviewWorkflowId] = interviewWorkflowIdType
  given ScalarType[SearchId] = searchIdType
  given EnumType[UserRole] = userRole
  given InputType[CandidateAvailabilityStatus] = candidateAvailabilityStatus

  // --- Enums ------------------------------------------------------------------------------------------------------

  lazy val jobStatus: EnumType[JobStatus] =
    describedEnum("JobStatus", "The lifecycle state of a job.", JobStatus.values.toList) {
      case JobStatus.Draft  => "Not yet visible to candidates."
      case JobStatus.Open   => "Visible to candidates, who may apply."
      case JobStatus.Closed => "No longer accepting applications."
    }
  lazy val applicationStatus: EnumType[ApplicationStatus] =
    describedEnum("ApplicationStatus", "The state of an application.", ApplicationStatus.values.toList) {
      case ApplicationStatus.Created   => "Submitted by the candidate."
      case ApplicationStatus.Accepted  => "Accepted by the recruiter for further steps."
      case ApplicationStatus.Declined  => "Declined by the candidate, with a reason."
      case ApplicationStatus.Interview => "An interview stage has started."
      case ApplicationStatus.Hired     => "The candidate was hired."
      case ApplicationStatus.Rejected  => "Rejected by the recruiter, with feedback."
    }
  lazy val userRole: EnumType[UserRole] =
    describedEnum("UserRole", "The role of a user account.", UserRole.values.toList) {
      case UserRole.Candidate => "Searches open jobs and applies to them."
      case UserRole.Recruiter => "Manages owned jobs and the applications to them."
      case UserRole.Admin     => "The singleton administrator."
    }
  lazy val userStatus: EnumType[AccountStatus] =
    describedEnum("UserStatus", "Whether a user account is usable.", AccountStatus.values.toList) {
      case AccountStatus.Active  => "The account can sign in."
      case AccountStatus.Deleted => "The account was deleted."
    }
  lazy val searchMode: EnumType[SearchMode] =
    describedEnum("SearchMode", "How a ranked result was found.", SearchMode.values.toList) {
      case SearchMode.FILTER => "Structured filters only."
      case SearchMode.VECTOR => "Semantic (embedding) similarity only."
      case SearchMode.HYBRID => "Lexical and semantic results combined."
    }
  lazy val candidateAvailabilityStatus: EnumType[CandidateAvailabilityStatus] =
    describedEnum(
      "CandidateAvailabilityStatus",
      "Whether a candidate is open to opportunities.",
      CandidateAvailabilityStatus.values.toList
    ) {
      case CandidateAvailabilityStatus.AVAILABLE_NOW => "Open to opportunities now."
      case CandidateAvailabilityStatus.UNAVAILABLE   => "Not open to opportunities."
    }
  lazy val accountDeletionStatusType: EnumType[AccountDeletionStatus] =
    describedEnum(
      "AccountDeletionStatus",
      "Progress of an account deletion request.",
      AccountDeletionStatus.values.toList
    ) {
      case AccountDeletionStatus.Pending  => "The deletion has been requested and is still being applied."
      case AccountDeletionStatus.Complete => "The deletion and the analytics erasure are complete."
      case AccountDeletionStatus.NotFound => "No deletion with this receipt exists for the caller."
    }
  lazy val interviewProgressType: EnumType[InterviewWorkflowPhase] =
    describedEnum(
      "InterviewProgress",
      "Where an interview workflow is in its scheduling, cancellation or reschedule steps.",
      InterviewWorkflowPhase.values.toList
    ) {
      case InterviewWorkflowPhase.ReservationPending         => "The calendar slot is being reserved."
      case InterviewWorkflowPhase.StatusCommitPending        => "The application is being moved to the interview stage."
      case InterviewWorkflowPhase.CompensationPending        => "A failed scheduling is being undone."
      case InterviewWorkflowPhase.NotificationsPending       => "Participants are being notified."
      case InterviewWorkflowPhase.Completed                  => "Scheduled; all steps finished."
      case InterviewWorkflowPhase.RepairRequired             => "A step failed and an Admin must repair the workflow."
      case InterviewWorkflowPhase.ProposalPending            => "A reschedule proposal is waiting for a response."
      case InterviewWorkflowPhase.CancelPending              => "The cancellation is being applied."
      case InterviewWorkflowPhase.CancelNotificationsPending => "Participants are being told about the cancellation."
      case InterviewWorkflowPhase.Cancelled                  => "The interview is cancelled."
      case InterviewWorkflowPhase.RescheduleHoldPending      => "The new slot is being held."
      case InterviewWorkflowPhase.RescheduleSwapPending      => "The old slot is being swapped for the new one."
      case InterviewWorkflowPhase.RescheduleCancelOldPending => "The old slot is being released."
      case InterviewWorkflowPhase.RescheduleNotificationsPending => "Participants are being told about the new time."
      case InterviewWorkflowPhase.RescheduleCompensationPending  => "A failed reschedule is being undone."
    }
  lazy val interviewParticipantType: EnumType[InterviewParticipant] =
    describedEnum("InterviewParticipant", "A party to an interview.", InterviewParticipant.values.toList) {
      case InterviewParticipant.Candidate => "The candidate."
      case InterviewParticipant.Recruiter => "The recruiter."
    }
  lazy val facetCoverageType: EnumType[String] =
    describedNames(
      "FacetCoverage",
      "How much of the match set the facet counts cover.",
      List(
        ("ALL_MATCHES", "ALL_MATCHES", "Counts cover every match."),
        ("TOP_MATCHES", "TOP_MATCHES", "Counts cover only the top matches; see coverageLimit.")
      )
    )

  // --- Interview inputs -------------------------------------------------------------------------------------------

  lazy val scheduleInterviewInputType: InputObjectType[ScheduleInterviewGraphQLInput] =
    deriveInputObjectType[ScheduleInterviewGraphQLInput](
      InputObjectTypeName("ScheduleInterviewInput"),
      InputObjectTypeDescription("Schedules an interview for an accepted application."),
      DocumentInputField("applicationId", "The accepted application the interview belongs to."),
      DocumentInputField("startsAt", "When the interview starts; must be in the future."),
      DocumentInputField("endsAt", "When the interview ends; must be after the start."),
      DocumentInputField("idempotencyKey", "Client-generated key; repeating it with the same input replays the result.")
    )
  lazy val repairInterviewInputType: InputObjectType[RepairInterviewGraphQLInput] =
    deriveInputObjectType[RepairInterviewGraphQLInput](
      InputObjectTypeName("RepairInterviewInput"),
      InputObjectTypeDescription("Admin repair of an interview workflow that needs attention."),
      DocumentInputField("workflowId", "The workflow to repair."),
      DocumentInputField("expectedRevision", "The revision the caller last read; a different one is rejected."),
      DocumentInputField("idempotencyKey", "Client-generated key; repeating it with the same input replays the result.")
    )
  lazy val interviewActionInputType: InputObjectType[InterviewActionGraphQLInput] =
    deriveInputObjectType[InterviewActionGraphQLInput](
      InputObjectTypeName("InterviewActionInput"),
      InputObjectTypeDescription(
        "The shared input of the interview cancel and reschedule actions. The actor comes from the access token."
      ),
      DocumentInputField("workflowId", "The workflow the action applies to."),
      DocumentInputField("expectedRevision", "The revision the caller last read; a different one is rejected."),
      DocumentInputField("idempotencyKey", "Client-generated key; repeating it with the same input replays the result.")
    )
  lazy val proposeInterviewRescheduleInputType: InputObjectType[ProposeInterviewRescheduleGraphQLInput] =
    deriveInputObjectType[ProposeInterviewRescheduleGraphQLInput](
      InputObjectTypeName("ProposeInterviewRescheduleInput"),
      InputObjectTypeDescription("Proposes a new time for a scheduled interview."),
      DocumentInputField("workflowId", "The workflow to reschedule."),
      DocumentInputField("expectedRevision", "The revision the caller last read; a different one is rejected."),
      DocumentInputField("startsAt", "The proposed start; must be in the future."),
      DocumentInputField("endsAt", "The proposed end; must be after the start."),
      DocumentInputField("idempotencyKey", "Client-generated key; repeating it with the same input replays the result.")
    )
  lazy val interviewActionInputArgument: Argument[InterviewActionGraphQLInput] =
    Argument("input", interviewActionInputType, "The action input.")
  lazy val proposeInterviewRescheduleInputArgument: Argument[ProposeInterviewRescheduleGraphQLInput] =
    Argument("input", proposeInterviewRescheduleInputType, "The proposal input.")
  lazy val scheduleInterviewInputArgument: Argument[ScheduleInterviewGraphQLInput] =
    Argument("input", scheduleInterviewInputType, "The scheduling input.")
  lazy val repairInterviewInputArgument: Argument[RepairInterviewGraphQLInput] =
    Argument("input", repairInterviewInputType, "The repair input.")
  lazy val workflowIdArgument: Argument[InterviewWorkflowId] =
    Argument("workflowId", interviewWorkflowIdType, "The workflow to read.")

  // --- Query arguments --------------------------------------------------------------------------------------------

  lazy val idArgument: Argument[JobId] = Argument("id", jobIdType, "The job to read.")
  lazy val jobIdArgument: Argument[JobId] = Argument("jobId", jobIdType, "The job.")
  lazy val queryArgument: Argument[String] = Argument("query", StringType, "The free-text search query.")
  lazy val applicationIdArgument: Argument[ApplicationId] =
    Argument("applicationId", applicationIdType, "The application.")
  lazy val firstArgument: Argument[Int] = Argument("first", IntType, "The page size; between 1 and 100.")
  lazy val afterArgument: Argument[Option[String]] =
    Argument("after", OptionInputType(StringType), "The cursor of the last item of the previous page.")
  lazy val analyticsFromArgument: Argument[Instant] =
    Argument("from", instantType, "Start of the reporting period (inclusive).")
  lazy val analyticsToArgument: Argument[Instant] =
    Argument("to", instantType, "End of the reporting period; at most 30 days after the start.")
  lazy val deletionReceiptIdArgument: Argument[String] =
    Argument("receiptId", deletionReceiptIdType, "The receipt returned by deleteMyAccount.")
  lazy val expectedModelArgument: Argument[Option[String]] =
    Argument("expectedModel", OptionInputType(StringType), "The embedding model the deployment is expected to use.")
  lazy val searchIdArgument: Argument[Option[SearchId]] =
    Argument(
      "searchId",
      OptionInputType(searchIdType),
      "Reuses a search identifier so repeated pages and later interactions correlate; the server issues one if absent."
    )
  lazy val candidateSearchQueryArgument: Argument[Option[String]] =
    Argument("query", OptionInputType(StringType), "Optional free-text query to rank candidates against.")
  lazy val candidateMatchFilterInputType: InputObjectType[CandidateMatchFilter] =
    deriveInputObjectType[CandidateMatchFilter](
      InputObjectTypeName("CandidateMatchFilter"),
      InputObjectTypeDescription("Narrows the candidates considered for a job."),
      DocumentInputField("requiredSkills", "Skills every candidate must have."),
      DocumentInputField("country", "Candidate's current residence country."),
      DocumentInputField("city", "Candidate's current residence city."),
      DocumentInputField("availabilityStatus", "Candidate availability.")
    )
  lazy val geoPointInputType: InputObjectType[GeoPointGraphQLInput] =
    deriveInputObjectType[GeoPointGraphQLInput](
      InputObjectTypeName("GeoPointInput"),
      InputObjectTypeDescription("A WGS84 coordinate."),
      DocumentInputField("latitude", "Degrees, from -90 to 90."),
      DocumentInputField("longitude", "Degrees, from -180 to 180.")
    )
  given InputType[GeoPointGraphQLInput] = geoPointInputType
  lazy val nearbyCenterArgument: Argument[GeoPointGraphQLInput] =
    Argument("center", geoPointInputType, "The point to search around.")
  lazy val radiusKmArgument: Argument[Double] = Argument("radiusKm", FloatType, "The search radius in kilometres.")
  lazy val optionalFacetQueryArgument: Argument[Option[String]] =
    Argument("query", OptionInputType(StringType), "Optional lexical query the facets are computed for.")
  lazy val optionalRadiusKmArgument: Argument[Option[Double]] =
    Argument("radiusKm", OptionInputType(FloatType), "The radius in kilometres; supplied together with center.")
  lazy val optionalNearbyCenterArgument: Argument[Option[GeoPointGraphQLInput]] =
    Argument("center", OptionInputType(geoPointInputType), "The point to restrict to; supplied together with radiusKm.")
  lazy val candidateMatchFilterArgument: Argument[Option[CandidateMatchFilter]] =
    Argument("filter", OptionInputType(candidateMatchFilterInputType), "Optional candidate filters.")
  lazy val jobStatusArgument: Argument[Option[JobStatus]] =
    Argument("status", OptionInputType(jobStatus), "Only jobs in this status.")
  lazy val applicationStatusArgument: Argument[Option[ApplicationStatus]] =
    Argument("status", OptionInputType(applicationStatus), "Only applications in this status.")
  lazy val userRoleArgument: Argument[Option[UserRole]] =
    Argument("role", OptionInputType(userRole), "Only users with this role.")
  lazy val userStatusArgument: Argument[Option[AccountStatus]] =
    Argument("status", OptionInputType(userStatus), "Only users with this status; defaults to ACTIVE.")
  lazy val jobFilterInputType: InputObjectType[JobFilterGraphQLInput] =
    deriveInputObjectType[JobFilterGraphQLInput](
      InputObjectTypeName("JobFilter"),
      InputObjectTypeDescription("Structured filters shared by every job discovery operation."),
      DocumentInputField("city", "Only jobs in this city; it must not be blank."),
      DocumentInputField("skills", "Only jobs that require all of these skills; at most 100, each of bounded length."),
      DocumentInputField("createdAfter", "Only jobs created after this instant.")
    )
  lazy val jobFilterArgument: Argument[Option[JobFilterGraphQLInput]] =
    Argument("filter", OptionInputType(jobFilterInputType), "Optional structured filters.")

  // --- Mutation inputs --------------------------------------------------------------------------------------------

  private val IdempotencyKeyDoc = "Client-generated key; repeating it with the same input replays the result."

  lazy val submitApplicationInputType: InputObjectType[SubmitApplicationGraphQLInput] =
    deriveInputObjectType[SubmitApplicationGraphQLInput](
      InputObjectTypeName("SubmitApplicationInput"),
      InputObjectTypeDescription("Applies to an open job as the calling candidate."),
      DocumentInputField("jobId", "The open job to apply to."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc)
    )
  lazy val createJobInputType: InputObjectType[CreateJobGraphQLInput] =
    deriveInputObjectType[CreateJobGraphQLInput](
      InputObjectTypeName("CreateJobInput"),
      InputObjectTypeDescription("Creates an open job owned by the calling recruiter."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("title", "The job title."),
      DocumentInputField("description", "The job description."),
      DocumentInputField("requirements", "Free-text requirements."),
      DocumentInputField("skills", "Skills the job requires."),
      DocumentInputField("country", "The job's country."),
      DocumentInputField("city", "The job's city, if it has one."),
      DocumentInputField("remote", "Whether the job can be done remotely."),
      DocumentInputField("coordinates", "Geographic position used by nearby search.")
    )
  lazy val jobInputType: InputObjectType[JobGraphQLInput] =
    deriveInputObjectType[JobGraphQLInput](
      InputObjectTypeName("JobInput"),
      InputObjectTypeDescription("The full editable content of a job. Updating a job replaces all of it."),
      DocumentInputField("title", "The job title."),
      DocumentInputField("description", "The job description."),
      DocumentInputField("requirements", "Free-text requirements."),
      DocumentInputField("skills", "Skills the job requires."),
      DocumentInputField("country", "The job's country."),
      DocumentInputField("city", "The job's city, if it has one."),
      DocumentInputField("remote", "Whether the job can be done remotely."),
      DocumentInputField("coordinates", "Geographic position used by nearby search.")
    )
  given InputType[JobGraphQLInput] = jobInputType
  lazy val updateJobInputType: InputObjectType[UpdateJobGraphQLInput] =
    deriveInputObjectType[UpdateJobGraphQLInput](
      InputObjectTypeName("UpdateJobInput"),
      InputObjectTypeDescription("Replaces the content of a job owned by the caller."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("id", "The job to update."),
      DocumentInputField("job", "The new content; it replaces the whole previous content, not a partial patch.")
    )
  lazy val jobActionInputType: InputObjectType[JobActionGraphQLInput] =
    deriveInputObjectType[JobActionGraphQLInput](
      InputObjectTypeName("JobActionInput"),
      InputObjectTypeDescription("A lifecycle action on a job owned by the caller."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("jobId", "The job.")
    )
  lazy val applicationActionInputType: InputObjectType[ApplicationActionGraphQLInput] =
    deriveInputObjectType[ApplicationActionGraphQLInput](
      InputObjectTypeName("ApplicationActionInput"),
      InputObjectTypeDescription("A status change of an application that needs no further data."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("applicationId", "The application.")
    )
  lazy val rejectApplicationInputType: InputObjectType[RejectApplicationGraphQLInput] =
    deriveInputObjectType[RejectApplicationGraphQLInput](
      InputObjectTypeName("RejectApplicationInput"),
      InputObjectTypeDescription("Rejects an application; feedback is required."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("applicationId", "The application."),
      DocumentInputField("feedback", "Why the application was rejected; required by the rule, checked on submit.")
    )
  lazy val declineApplicationInputType: InputObjectType[DeclineApplicationGraphQLInput] =
    deriveInputObjectType[DeclineApplicationGraphQLInput](
      InputObjectTypeName("DeclineApplicationInput"),
      InputObjectTypeDescription("Declines an application as its candidate; a reason is required."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("applicationId", "The application."),
      DocumentInputField("reason", "Why the candidate declines; required by the rule, checked on submit.")
    )
  lazy val submitApplicationInputArgument: Argument[SubmitApplicationGraphQLInput] =
    Argument("input", submitApplicationInputType, "The application to submit.")
  lazy val createJobInputArgument: Argument[CreateJobGraphQLInput] =
    Argument("input", createJobInputType, "The job to create.")
  lazy val updateJobInputArgument: Argument[UpdateJobGraphQLInput] =
    Argument("input", updateJobInputType, "The job update.")
  lazy val jobActionInputArgument: Argument[JobActionGraphQLInput] =
    Argument("input", jobActionInputType, "The job action.")
  lazy val applicationActionInputArgument: Argument[ApplicationActionGraphQLInput] =
    Argument("input", applicationActionInputType, "The application action.")
  lazy val rejectApplicationInputArgument: Argument[RejectApplicationGraphQLInput] =
    Argument("input", rejectApplicationInputType, "The rejection.")
  lazy val declineApplicationInputArgument: Argument[DeclineApplicationGraphQLInput] =
    Argument("input", declineApplicationInputType, "The decline.")

  // --- Account inputs ---------------------------------------------------------------------------------------------

  lazy val residenceInputType: InputObjectType[ResidenceGraphQLInput] =
    deriveInputObjectType[ResidenceGraphQLInput](
      InputObjectTypeName("ResidenceInput"),
      InputObjectTypeDescription("Where a candidate currently lives."),
      DocumentInputField("country", "The country."),
      DocumentInputField("city", "The city, if known.")
    )
  given InputType[ResidenceGraphQLInput] = residenceInputType
  lazy val candidateProfileInputType: InputObjectType[CandidateProfileGraphQLInput] =
    deriveInputObjectType[CandidateProfileGraphQLInput](
      InputObjectTypeName("CandidateProfileInput"),
      InputObjectTypeDescription("The profile of a candidate."),
      DocumentInputField("skills", "The candidate's skills."),
      DocumentInputField("experienceSummary", "A short summary of experience."),
      DocumentInputField("resumeRef", "A reference to the stored resume."),
      DocumentInputField("residence", "Where the candidate lives; omitting it clears the stored residence."),
      DocumentInputField("availabilityStatus", "Whether the candidate is open to opportunities."),
      DocumentInputField("recruiterSearchOptIn", "Whether recruiters may find this candidate; defaults to false.")
    )
  given InputType[CandidateProfileGraphQLInput] = candidateProfileInputType
  lazy val recruiterProfileInputType: InputObjectType[RecruiterProfileGraphQLInput] =
    deriveInputObjectType[RecruiterProfileGraphQLInput](
      InputObjectTypeName("RecruiterProfileInput"),
      InputObjectTypeDescription("The profile of a recruiter."),
      DocumentInputField("organizationName", "The organization the recruiter hires for."),
      DocumentInputField("jobTitle", "The recruiter's job title.")
    )
  given InputType[RecruiterProfileGraphQLInput] = recruiterProfileInputType
  lazy val profileInputType: InputObjectType[ProfileGraphQLInput] =
    deriveInputObjectType[ProfileGraphQLInput](
      InputObjectTypeName("ProfileInput"),
      InputObjectTypeDescription(
        "A role-specific profile. Exactly one of candidate and recruiter must be set, and it must match the account's role."
      ),
      DocumentInputField("candidate", "The profile of a Candidate account."),
      DocumentInputField("recruiter", "The profile of a Recruiter account.")
    ).copy(astDirectives = Vector(sangria.ast.Directive("oneOf")))
  given InputType[ProfileGraphQLInput] = profileInputType
  lazy val signUpInputType: InputObjectType[SignUpGraphQLInput] =
    deriveInputObjectType[SignUpGraphQLInput](
      InputObjectTypeName("SignUpInput"),
      InputObjectTypeDescription("Creates a Candidate or Recruiter account."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("name", "The unique sign-in name."),
      DocumentInputField("role", "CANDIDATE or RECRUITER; ADMIN accounts come from configuration only."),
      DocumentInputField("password", "The password."),
      DocumentInputField("profile", "The role-specific profile; required for Candidate and Recruiter.")
    )
  lazy val loginInputType: InputObjectType[LoginGraphQLInput] =
    deriveInputObjectType[LoginGraphQLInput](
      InputObjectTypeName("LoginInput"),
      InputObjectTypeDescription("Credentials for signing in."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("name", "The sign-in name."),
      DocumentInputField("password", "The password.")
    )
  lazy val updateProfileInputType: InputObjectType[UpdateProfileGraphQLInput] =
    deriveInputObjectType[UpdateProfileGraphQLInput](
      InputObjectTypeName("UpdateMyProfileInput"),
      InputObjectTypeDescription("Replaces the caller's profile."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("profile", "The new profile; it replaces the whole stored profile, not a partial update.")
    )
  lazy val signUpInputArgument: Argument[SignUpGraphQLInput] =
    Argument("input", signUpInputType, "The account to create.")
  lazy val loginInputArgument: Argument[LoginGraphQLInput] = Argument("input", loginInputType, "The credentials.")
  lazy val updateProfileInputArgument: Argument[UpdateProfileGraphQLInput] =
    Argument("input", updateProfileInputType, "The new profile.")
  lazy val deleteMyAccountInputType: InputObjectType[DeleteMyAccountGraphQLInput] =
    deriveInputObjectType[DeleteMyAccountGraphQLInput](
      InputObjectTypeName("DeleteMyAccountInput"),
      InputObjectTypeDescription("Requests deletion of the caller's account."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc)
    )
  lazy val deleteMyAccountInputArgument: Argument[DeleteMyAccountGraphQLInput] =
    Argument("input", deleteMyAccountInputType, "The deletion request.")

  // --- Interaction inputs -----------------------------------------------------------------------------------------

  lazy val recordJobViewInputType: InputObjectType[RecordJobViewGraphQLInput] =
    deriveInputObjectType[RecordJobViewGraphQLInput](
      InputObjectTypeName("RecordJobViewInput"),
      InputObjectTypeDescription("Records that the caller viewed a job."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("eventId", "Client-generated identifier of this interaction event."),
      DocumentInputField("jobId", "The viewed job."),
      DocumentInputField("searchId", "The search that led to the view, if any.")
    )
  lazy val recordSearchResultClickInputType: InputObjectType[RecordSearchResultClickGraphQLInput] =
    deriveInputObjectType[RecordSearchResultClickGraphQLInput](
      InputObjectTypeName("RecordSearchResultClickInput"),
      InputObjectTypeDescription("Records that the caller opened a result of a search."),
      DocumentInputField("idempotencyKey", IdempotencyKeyDoc),
      DocumentInputField("eventId", "Client-generated identifier of this interaction event."),
      DocumentInputField("searchId", "The search that produced the result."),
      DocumentInputField("resultId", "The identifier of the clicked result.")
    )
  lazy val recordJobViewInputArgument: Argument[RecordJobViewGraphQLInput] =
    Argument("input", recordJobViewInputType, "The view to record.")
  lazy val recordSearchResultClickInputArgument: Argument[RecordSearchResultClickGraphQLInput] =
    Argument("input", recordSearchResultClickInputType, "The click to record.")
}
