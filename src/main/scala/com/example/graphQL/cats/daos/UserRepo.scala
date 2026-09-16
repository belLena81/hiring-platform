package com.example.graphQL.cats.daos

import cats.effect._
import doobie._
import doobie.implicits._
import com.example.graphQL.cats.models.{Role, User}


abstract class UserRepo[F[_]] {
  def fetchAll(): F[List[User]]
  def fetchById(id: String): F[Option[User]]
  def fetchByRole(role: Role): F[List[User]]
}

object UserRepo {
  implicit val roleGet: Get[Role] = Get[String].map(Role.fromRole)
  implicit val natPut: Put[Role] = Put[String].contramap(Role.toRole)

  def fromTransactor[F[_]: Async](xa: Transactor[F]): UserRepo[F] =
    new UserRepo[F] {

      val select: Fragment =
        fr"""
          SELECT *
          FROM   users
        """

      def fetchAll(): F[List[User]] =
        select.query[User].to[List].transact[F](xa)

      def fetchById(id: String): F[Option[User]] =
        (select ++ sql"WHERE id = $id").query[User].option.transact[F](xa)

      def fetchByRole(role: Role): F[List[User]] =
        (select ++ sql"WHERE role = ${role.name}").query[User].to[List].transact[F](xa)

    }

}
