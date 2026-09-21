package com.example.graphQL.cats.api.http

import cats.effect.IO
import com.comcast.ip4s.{Cidr, IpAddress, SocketAddress}
import com.example.graphQL.cats.config.TrustedProxyConfig
import munit.FunSuite
import org.http4s.{Header, Headers, Request}
import org.typelevel.ci.CIString

final class ClientAddressResolverSpec extends FunSuite {
  private def ip(value: String): IpAddress = IpAddress.fromString(value).get

  private def resolver(cidrs: String*): ClientAddressResolver =
    ClientAddressResolver(TrustedProxyConfig(cidrs.toList.map(Cidr.fromString(_).get)))

  private def request(peer: Option[String], forwarded: List[String], xForwardedFor: List[String] = Nil): Request[IO] = {
    val headers = forwarded.map(value => Header.Raw(CIString("Forwarded"), value)) ++
      xForwardedFor.map(value => Header.Raw(CIString("X-Forwarded-For"), value))
    val base = Request[IO](headers = Headers(headers))
    peer.fold(base) { value =>
      val socket = if (value.contains(':')) s"[$value]:12345" else s"$value:12345"
      base.withAttribute(Request.Keys.ConnectionInfo, Request.Connection(
        SocketAddress.fromStringIp("127.0.0.1:8080").get,
        SocketAddress.fromStringIp(socket).get,
        secure = false
      ))
    }
  }

  test("direct and untrusted peers ignore Forwarded") {
    val forwarded = List("for=203.0.113.40")
    assertEquals(resolver().resolve(request(Some("198.51.100.10"), forwarded)), Some(ip("198.51.100.10")))
    assertEquals(resolver("10.0.0.0/8").resolve(request(Some("198.51.100.10"), forwarded)), Some(ip("198.51.100.10")))
  }

  test("trusted proxy selects the nearest non-proxy Forwarded address") {
    val result = resolver("10.0.0.0/8").resolve(request(Some("10.0.0.5"),
      List("for=198.51.100.99, for=203.0.113.40")))
    assertEquals(result, Some(ip("203.0.113.40")))
  }

  test("trusted proxy discards configured intermediary hops") {
    val result = resolver("10.0.0.0/8").resolve(request(Some("10.0.0.5"),
      List("for=198.51.100.99, for=10.1.2.3")))
    assertEquals(result, Some(ip("198.51.100.99")))
  }

  test("trusted proxy falls back to its TCP peer for unusable Forwarded chains") {
    val addressResolver = resolver("10.0.0.0/8")
    List(
      "for=unknown",
      "for=_hidden",
      "by=10.1.2.3",
      "for=not-an-address"
    ).foreach { header =>
      assertEquals(addressResolver.resolve(request(Some("10.0.0.5"), List(header))), Some(ip("10.0.0.5")), clues(header))
    }
  }

  test("multiple Forwarded lines are one chain and skip unusable members") {
    val addressResolver = resolver("10.0.0.0/8")
    assertEquals(addressResolver.resolve(request(Some("10.0.0.5"),
      List("for=198.51.100.99", "for=10.1.2.3"))), Some(ip("198.51.100.99")))
    assertEquals(addressResolver.resolve(request(Some("10.0.0.5"),
      List("for=198.51.100.99", "for=unknown"))), Some(ip("198.51.100.99")))
    assertEquals(addressResolver.resolve(request(Some("10.0.0.5"),
      List("for=unknown", "for=203.0.113.40"))), Some(ip("203.0.113.40")))
    assertEquals(addressResolver.resolve(request(Some("10.0.0.5"),
      List("for=198.51.100.99, for=unknown, for=10.1.2.3"))), Some(ip("198.51.100.99")))
  }

  test("IPv6 proxy CIDRs and Forwarded addresses are supported") {
    val result = resolver("2001:db8:10::/64").resolve(request(Some("2001:db8:10::5"),
      List("for=\"[2001:db8:ffff::40]\"")))
    assertEquals(result, Some(ip("2001:db8:ffff::40")))
  }

  test("missing TCP peer never trusts Forwarded") {
    assertEquals(resolver("10.0.0.0/8").resolve(request(None, List("for=203.0.113.40"))), None)
  }

  test("trusted proxy uses typed X-Forwarded-For when Forwarded is absent") {
    val result = resolver("10.0.0.0/8").resolve(request(Some("10.0.0.5"), Nil,
      List("198.51.100.99, 10.1.2.3")))
    assertEquals(result, Some(ip("198.51.100.99")))
  }

  test("Forwarded takes precedence over X-Forwarded-For") {
    val result = resolver("10.0.0.0/8").resolve(request(Some("10.0.0.5"),
      List("for=203.0.113.40"), List("198.51.100.99")))
    assertEquals(result, Some(ip("203.0.113.40")))
  }

  test("untrusted peers cannot use X-Forwarded-For") {
    val result = resolver("10.0.0.0/8").resolve(request(Some("198.51.100.10"), Nil,
      List("203.0.113.40")))
    assertEquals(result, Some(ip("198.51.100.10")))
  }
}
