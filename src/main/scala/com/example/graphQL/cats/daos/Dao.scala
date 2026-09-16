package com.example.graphQL.cats.daos

import cats.effect._
import com.example.graphQL.cats.models.{Role, User}
import doobie.util.transactor.Transactor

import scala.concurrent.Future


final case class Dao[F[_]](users: UserRepo[F])

object Dao {

  def fromTransactor[F[_]: Async](xa: Transactor[F]): Dao[F] =
    Dao(
      UserRepo.fromTransactor(xa)
    )

}

case class FutureDao(ioDao: Dao[IO]) {
  import cats.effect.unsafe.implicits.global

  def fetchAll(): Future[List[User]] = ioDao.users.fetchAll().unsafeToFuture()
  def fetchById(id: String): Future[Option[User]] = ioDao.users.fetchById(id).unsafeToFuture()
  def fetchByRole(role: Role): Future[List[User]] = ioDao.users.fetchByRole(role).unsafeToFuture()
}
