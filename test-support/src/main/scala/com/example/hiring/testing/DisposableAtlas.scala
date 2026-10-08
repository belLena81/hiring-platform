package com.example.hiring.testing

import com.mongodb.ConnectionString
import scala.jdk.CollectionConverters.*

/** Explicit authorization for synthetic writes to an exact, operator-selected test deployment. */
object DisposableAtlas {
  private val Loopback = Set("localhost", "127.0.0.1", "::1", "[::1]")

  /** Evidence label: a loopback-only deployment is a local container, never an Atlas measurement. */
  def deployment(uri: String): String =
    scala.util
      .Try(new ConnectionString(uri).getHosts.asScala.toList)
      .toOption
      .filter(hosts =>
        hosts.nonEmpty && hosts.forall(host => Loopback.contains(host.replaceAll(":\\d+$", "").toLowerCase))
      )
      .fold("atlas")(_ => "local-container")

  def authorizedUri(environment: Map[String, String]): Either[String, String] = {
    val failure =
      "Disposable Atlas requires ATLAS_TEST_URI, ATLAS_TEST_DISPOSABLE=true and exact ATLAS_TEST_ALLOWED_HOSTS"
    for {
      uri <- environment.get("ATLAS_TEST_URI").filter(_.nonEmpty).toRight(failure)
      _ <- Either.cond(environment.get("ATLAS_TEST_DISPOSABLE").contains("true"), (), failure)
      allowed <- environment.get("ATLAS_TEST_ALLOWED_HOSTS").toRight(failure)
      _ <- Either.cond(
        allowed.split(",", -1).forall(host => host.nonEmpty && host == host.trim && !host.contains("*")),
        (),
        failure
      )
      connection <- scala.util.Try(new ConnectionString(uri)).toEither.left.map(_ => failure)
      _ <- Either.cond(Option(connection.getDatabase).isEmpty, (), failure)
      hosts = connection.getHosts.asScala.toSet
      _ <- Either.cond(hosts.nonEmpty && hosts == allowed.split(",", -1).toSet, (), failure)
    } yield uri
  }
}
