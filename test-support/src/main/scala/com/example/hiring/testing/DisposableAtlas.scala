package com.example.hiring.testing

import com.mongodb.ConnectionString
import scala.jdk.CollectionConverters.*

/** Explicit authorization for synthetic writes to an exact, operator-selected test deployment. */
object DisposableAtlas {
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
