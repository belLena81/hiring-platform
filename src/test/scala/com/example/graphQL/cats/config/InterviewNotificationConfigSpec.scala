package com.example.graphQL.cats.config

import munit.FunSuite

class InterviewNotificationConfigSpec extends FunSuite {
  private val prefix = "kafka.interview.notifications"
  private def configuration(overrides: String) = AppConfigFixtures.withPackagedDefaults(overrides)
  private def errorsOf(overrides: String): List[ConfigError] =
    configuration(overrides).swap.toOption.fold(fail(s"Expected rejected configuration for $overrides"))(_.toList)
  private def selection(overrides: String) =
    configuration(overrides).map(_.kafka.interview.notifications.selection)

  private val validSes =
    s"""$prefix.provider = "ses"
       |$prefix.ses.enabled = true
       |$prefix.ses.region = "eu-west-1"
       |$prefix.ses.from-address = "hiring@example.invalid"
       |""".stripMargin

  test("DHW-26 the packaged defaults select the ledger provider and SES stays disabled") {
    assertEquals(selection(""), Right(InterviewNotificationSelection.Ledger))
  }

  test("DHW-26 SES is selected only when provider = ses and ses.enabled = true, with explicit settings") {
    selection(validSes + s"""$prefix.ses.configuration-set = "hiring-interviews"\n""") match {
      case Right(InterviewNotificationSelection.Ses(settings)) =>
        assertEquals(settings.region.toString, "eu-west-1")
        assertEquals(settings.configurationSet.map(_.toString), Some("hiring-interviews"))
      case other => fail(s"Expected SES selection, got $other")
    }
  }

  test("DHW-26 double opt-in: ledger with ses settings present but disabled stays on the ledger") {
    val settingsOnly =
      s"""$prefix.ses.region = "eu-west-1"
         |$prefix.ses.from-address = "hiring@example.invalid"
         |""".stripMargin
    assertEquals(selection(settingsOnly), Right(InterviewNotificationSelection.Ledger))
  }

  test("DHW-26 provider = ses with ses.enabled = false is a startup error, not a fallback to the ledger") {
    assert(
      errorsOf(s"""$prefix.provider = "ses"\n""").contains(ConfigError.InvalidInterviewSesSelection)
    )
  }

  test("DHW-26 provider = ledger with ses.enabled = true is a startup error, not a silent disable") {
    assert(errorsOf(s"$prefix.ses.enabled = true\n").contains(ConfigError.InvalidInterviewSesSelection))
  }

  test("DHW-26 a selected SES without region and sender reports both, accumulated") {
    val errors = errorsOf(s"""$prefix.provider = "ses"\n$prefix.ses.enabled = true\n""")
    assert(errors.contains(ConfigError.InvalidInterviewSesRegion), clues(errors))
    assert(errors.contains(ConfigError.InvalidInterviewSesFromAddress), clues(errors))
  }

  test("DHW-26 independent invalid settings accumulate instead of stopping at the first") {
    val errors = errorsOf(
      s"""$prefix.provider = "smtp"
         |$prefix.ses.region = "Europe"
         |$prefix.ses.from-address = "a@b.test\\r\\nBcc: x@y.test"
         |$prefix.ses.configuration-set = "bad name!"
         |""".stripMargin
    )
    List(
      ConfigError.InvalidInterviewNotificationProvider,
      ConfigError.InvalidInterviewSesRegion,
      ConfigError.InvalidInterviewSesFromAddress,
      ConfigError.InvalidInterviewSesConfigurationSet
    ).foreach(error => assert(errors.contains(error), clues(error, errors)))
  }

  test("DHW-26 forbidden input: malformed regions and sender addresses are refused") {
    List("us-east", "EU-WEST-1", "eu_west_1", "", "eu-west-1 ", "eu-west-1\n", "https://email.eu-west-1.example")
      .foreach { region =>
        assert(
          errorsOf(s"""$prefix.ses.region = "${region.replace("\n", "\\n")}"\n""")
            .contains(ConfigError.InvalidInterviewSesRegion),
          clues(region)
        )
      }
    List("no-at-sign", "a@b.test, c@d.test", "<a@b.test>", "a b@c.test", "").foreach { sender =>
      assert(
        errorsOf(s"""$prefix.ses.from-address = "$sender"\n""").contains(ConfigError.InvalidInterviewSesFromAddress),
        clues(sender)
      )
    }
  }

  List(
    "access-key-id",
    "secret-access-key",
    "session-token",
    "profile",
    "credentials",
    "endpoint"
  ).foreach { key =>
    test(s"DHW-26 forbidden input: ses.$key fails at startup, even when SES is not selected") {
      val errors = errorsOf(s"""$prefix.ses.$key = "placeholder"\n""")
      assert(errors.contains(ConfigError.ForbiddenInterviewSesSetting), clues(key, errors))
    }
  }

  test("DHW-26 an unknown key in the notifications section also fails closed") {
    assert(errorsOf(s"""$prefix.endpoint = "https://example.invalid"\n""").nonEmpty)
    assert(errorsOf(s"""$prefix.ses.retries = 3\n""").nonEmpty)
  }

  test("DHW-26 an unknown key at notifications level does not hide a forbidden ses key or another invalid setting") {
    val errors = errorsOf(
      s"""$prefix.endpoint = "https://example.invalid"
         |$prefix.ses.secret-access-key = "placeholder"
         |$prefix.ses.region = "Europe"
         |""".stripMargin
    )
    assert(errors.contains(ConfigError.ForbiddenInterviewSesSetting), clues(errors))
    assert(errors.contains(ConfigError.InvalidInterviewSesRegion), clues(errors))
    assert(errors.contains(ConfigError.InvalidConfigFile), clues(errors))
  }

  test("DHW-26 forbidden input: a sender with a Unicode separator, control or format character is refused") {
    List(
      "NEL" -> "\\u0085",
      "line separator" -> "\\u2028",
      "paragraph separator" -> "\\u2029",
      "no-break space" -> "\\u00A0",
      "bidi override" -> "\\u202E",
      "zero-width space" -> "\\u200B"
    ).foreach { case (label, character) =>
      List(s"a${character}b@example.test", s"a@exa${character}mple.test").foreach { sender =>
        assert(
          errorsOf(s"""$prefix.ses.from-address = "$sender"\n""").contains(ConfigError.InvalidInterviewSesFromAddress),
          clues(label, sender)
        )
      }
    }
  }

  test("DHW-26 the notification config values never render the sender address or the configuration set name") {
    val config = configuration(validSes + s"""$prefix.ses.configuration-set = "hiring-interviews"\n""")
      .fold(errors => fail(errors.toString), identity)
    val rendered = List(
      config.kafka.interview.notifications.toString,
      config.kafka.interview.toString,
      config.kafka.toString
    ).mkString(" ")
    assert(!rendered.contains("hiring@example.invalid"), clues(rendered))
    assert(!rendered.contains("hiring-interviews"), clues(rendered))
    assert(config.kafka.interview.notifications.toString.contains("[REDACTED]"))
    config.kafka.interview.notifications.selection match {
      case InterviewNotificationSelection.Ses(settings) =>
        assert(settings.toString.contains("[REDACTED]"))
        assert(!settings.toString.contains("hiring@example.invalid"))
      case other => fail(s"Expected SES selection, got $other")
    }
  }
}
