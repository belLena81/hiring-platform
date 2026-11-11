package com.example.graphQL.cats.config

import com.example.graphQL.cats.domain.model.EmailAddress
import io.github.iltotore.iron.*
import io.github.iltotore.iron.pureconfig.RefinedConfigError
import _root_.pureconfig.*
import _root_.pureconfig.error.{CannotConvert, ConfigReaderFailure, ConfigReaderFailures, ConvertFailure, UnknownKey}

/** Readers for `kafka.interview.notifications`.
  *
  * The Scala 3 derivation of pureconfig 0.17 has no `ProductHint`, so unknown keys would be ignored silently. `strict`
  * restores the `allowUnknownKeys = false` behavior: every key outside the declared set fails decoding with an
  * `UnknownKey` failure at its own path. That is what makes a credential or endpoint key (for example
  * `ses.secret-access-key`) a startup error instead of an ignored line, which matters because credentials may only come
  * from the runtime credential chain.
  */
private object InterviewNotificationReaders {
  private def refined[A, C](using
      reader: ConfigReader[A],
      constraint: RuntimeConstraint[A, C]
  ): ConfigReader[A :| C] =
    reader.emap(_.refineEither[C].left.map(RefinedConfigError(_)))

  given ConfigReader[SesRegion] = refined
  given ConfigReader[SesConfigurationSetName] = refined

  /** The existing `EmailAddress` validator, plus the header-safety rule for a sender: one `local@domain`, no
    * whitespace, control, angle-bracket, comma, semicolon or quote characters.
    */
  given ConfigReader[EmailAddress] =
    ConfigReader[String].emap { raw =>
      EmailAddress
        .from(raw)
        .left
        .map(_ => CannotConvert("[redacted]", "EmailAddress", "invalid"))
        .filterOrElse(
          address => SesMailboxAddress.matches(address.value),
          CannotConvert("[redacted]", "EmailAddress", "invalid")
        )
    }

  given ConfigReader[InterviewNotificationProviderKind] =
    ConfigReader[String].emap(value =>
      InterviewNotificationProviderKind.values
        .find(_.wire == value)
        .toRight(CannotConvert(value, "InterviewNotificationProviderKind", "expected ledger or ses"))
    )

  /** Unknown keys and the inner reader's own failures are reported together, so one unknown key never hides another
    * invalid setting (including an unknown key inside a nested strict section).
    */
  def strict[A](allowed: Set[String])(inner: ConfigReader[A]): ConfigReader[A] =
    ConfigReader.fromCursor { cursor =>
      cursor.asObjectCursor.flatMap { objectCursor =>
        val unknownKeys = objectCursor.map.keys.toList.filterNot(allowed.contains).sorted.map { key =>
          val member = objectCursor.atKeyOrUndefined(key)
          ConvertFailure(UnknownKey(key), member.origin, member.path): ConfigReaderFailure
        }
        (unknownKeys, inner.from(cursor)) match {
          case (Nil, decoded)         => decoded
          case (_, Right(_))          => Left(ConfigReaderFailures(unknownKeys.head, unknownKeys.tail*))
          case (_, Left(innerErrors)) =>
            Left(ConfigReaderFailures(unknownKeys.head, unknownKeys.tail ++ innerErrors.toList*))
        }
      }
    }
}
import InterviewNotificationReaders.given

private[config] given ConfigReader[RawSesConfig] =
  InterviewNotificationReaders.strict(Set("enabled", "region", "from-address", "configuration-set"))(
    ConfigReader.forProduct4("enabled", "region", "from-address", "configuration-set")(
      (
          enabled: Option[Boolean],
          region: Option[SesRegion],
          fromAddress: Option[EmailAddress],
          configurationSet: Option[SesConfigurationSetName]
      ) => RawSesConfig(enabled.getOrElse(false), region, fromAddress, configurationSet)
    )
  )

private[config] given ConfigReader[RawInterviewNotificationConfig] =
  InterviewNotificationReaders.strict(Set("provider", "ses"))(
    ConfigReader.forProduct2("provider", "ses")(
      (provider: Option[InterviewNotificationProviderKind], ses: Option[RawSesConfig]) =>
        RawInterviewNotificationConfig(
          provider.getOrElse(InterviewNotificationProviderKind.Ledger),
          ses.getOrElse(RawSesConfig())
        )
    )
  )

/** Decoded, not yet cross-checked, SES settings. Absent values stay absent; selection rules live in validation. */
private[config] final case class RawSesConfig(
    enabled: Boolean = false,
    region: Option[SesRegion] = None,
    fromAddress: Option[EmailAddress] = None,
    configurationSet: Option[SesConfigurationSetName] = None
)

private[config] final case class RawInterviewNotificationConfig(
    provider: InterviewNotificationProviderKind = InterviewNotificationProviderKind.Ledger,
    ses: RawSesConfig = RawSesConfig()
)
