package com.example.graphQL.cats.infrastructure.events

/** Pure header builder for interview-step Kafka records: content type, schema version and the interview step. Headers
  * are optional metadata; the JSON body stays authoritative.
  */
object InterviewMessageHeaders {
  val InterviewStep: String = "hiring-step"

  /** Consumer-side check of present contract headers against the decoded body, with the operational record rules:
    * duplicate keys, values over 256 bytes and non-UTF-8 contract values are malformed; an unsupported version, a
    * non-JSON content type or a `hiring-step` that differs from the body's step are refused; absent contract headers
    * pass.
    */
  def verifyInterview(
      headers: OperationalEventHeaders.RecordHeaders,
      step: String
  ): Either[OperationalEventHeaders.HeaderRefusal, Unit] =
    OperationalEventHeaders.verifyContract(
      headers,
      OperationalEventHeaders.ContractKeys + InterviewStep,
      List((InterviewStep, step, OperationalEventHeaders.HeaderRefusal.InterviewStepMismatch))
    )

  /** True when the JSON text repeats an object key at any depth; the text must already be valid JSON. */
  def hasDuplicateKeys(json: String): Boolean = JsonDuplicateKeys.present(json)

  /** `extra` (for example propagated trace headers) cannot replace the fixed keys. */
  def interview(step: String, extra: List[(String, String)] = Nil): List[(String, Array[Byte])] =
    OperationalEventHeaders.withFixed(
      List(
        OperationalEventHeaders.ContentType -> OperationalEventHeaders.ContentTypeJson,
        OperationalEventHeaders.SchemaVersion -> OperationalEventHeaders.CurrentSchemaVersion,
        InterviewStep -> step
      ),
      extra
    )
}
