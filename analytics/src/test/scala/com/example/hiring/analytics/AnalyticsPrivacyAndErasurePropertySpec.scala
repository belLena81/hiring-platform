package com.example.hiring.analytics
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.service.erasure.*

import java.util.Base64
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

final class AnalyticsPrivacyAndErasurePropertySpec extends ScalaCheckSuite {
  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
  property("erasure claims accept only the immediate successor and reset phase progress") {
    forAll(
      Gen.choose(0, ErasurePhase.values.length - 1),
      Gen.choose(0, ErasurePhase.values.length - 1),
      Gen.choose(0, 1000000)
    ) { (phaseIndex, nextIndex, progress) =>
      val phase = ErasurePhase.values(phaseIndex)
      val requested = ErasurePhase.values(nextIndex)
      val claim = ErasureClaim(
        asAccountSubjectId("00000000-0000-0000-0000-000000000001"),
        "lease",
        java.time.Instant.EPOCH,
        phase,
        progress,
        progress.toLong
      )
      claim.advanceTo(requested) match {
        case Right(advanced) =>
          phase.next.contains(requested) && advanced.phase == requested && advanced.progress == 0 &&
          advanced.progressKey == requested.ordinal.toLong * ErasurePhase.ProgressPerPhase &&
          advanced.requestId == claim.requestId && advanced.leaseToken == claim.leaseToken
        case Left(_) => !phase.next.contains(requested)
      }
    }
  }

  property("Iron subject tokens accept only versioned base64url HMAC tokens") {
    forAll { (keyId: String, digest: String) =>
      val safeKeyId = keyId.filter(c => asciiAlphaNumeric(c) || c == '-').take(40)
      val safeDigest = digest.filter(c => asciiAlphaNumeric(c) || c == '_' || c == '-').take(43).padTo(43, 'A')
      val token = safeKeyId match {
        case "" => "hmac-v1_" + safeDigest
        case id => id + "_" + safeDigest
      }
      SubjectToken.fromHmac(token).isRight && SubjectToken.fromHmac("invalid-token").isLeft
    }
  }

  test("Iron subject tokens expose their validated string without an unchecked cast") {
    val token = "hmac-v1_" + "A" * 43

    assertEquals(SubjectToken.fromHmac(token).map(_.value), Right(token))
  }

  property("generated 256-bit HMAC configurations parse through the runtime boundary") {
    forAll(Gen.listOfN(32, Gen.choose(0, 255))) { generated =>
      val bytes = Array.tabulate[Byte](32)(index => generated.lift(index).getOrElse(0).toByte)
      val encoded = Base64.getEncoder.encodeToString(bytes)
      val config = runtimeConfig(encoded)
      AnalyticsConfigFixtures.batch(config).isRight
    }
  }

  property("short or malformed HMAC configuration is rejected without echoing the value") {
    forAll(Gen.choose(1, 31), Gen.alphaNumStr) { (length, marker) =>
      val encoded = Base64.getEncoder.encodeToString(Array.fill[Byte](length)(1))
      val invalid = runtimeConfig(encoded)
      val shortRejected = AnalyticsConfigFixtures.batch(invalid).swap.toOption.exists { error =>
        error.getMessage.contains("at least 32 bytes") && !error.getMessage.contains(encoded)
      }
      val malformedValue = marker + "%%%"
      val malformed = AnalyticsConfigFixtures.batch(runtimeConfig(malformedValue))
      shortRejected && malformed.isLeft && !malformed.swap.toOption.exists(_.getMessage.contains(malformedValue))
    }
  }

  private def runtimeConfig(encodedKey: String): String =
    s"""analytics {
       | mongo { uri = "mongodb://localhost:27017/?replicaSet=rs0", database = "hiring" }
       | spark { master = "local[*]", local-directory = "/var/lib/hiring-analytics/spark-temp/runtime-test" }
       | kafka { bootstrap-servers = "localhost:9092", username = "reader", password = "reader-secret", topic = "hiring.operational-events", fencer { username = "fencer", password = "fencer-secret" } }
       | lakehouse { root = "file:///tmp/hiring-analytics" }
       | hmac { secret-base64 = "$encodedKey", key-id = "hmac-v1" }
       | batch { run-id = "property-run", partition = "0", start-offset = "0", end-offset-exclusive = "1" }
       | operational {
       |   retention { bronze-days = 7, quarantine-days = 7, silver-days = 30, published-snapshot-days = 30, deletion-marker-days = 31, delta-vacuum-safety = 7 days, delta-log-retention = 30 days }
       |   report-reservation-ttl = 90 days
       |   mongo-transaction-window = 120 seconds
       |   maximum-erasure-evidence-files = 100000
|   mongo-publisher-buffer-size = 256
|   erasure-worker { lease-duration = 90 seconds, delivery-timeout = 30 seconds, poll-interval = 5 seconds }
| }
       |}""".stripMargin

  private def asciiAlphaNumeric(value: Char): Boolean =
    (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9')
}
