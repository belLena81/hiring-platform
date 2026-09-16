package com.example.graphQL.cats.schemas

import sangria.schema._
import com.example.graphQL.cats.daos.FutureDao
import com.example.graphQL.cats.models.Role

object QueryType {

  val id: Argument[String] =
    Argument(
      name = "identifier",
      argumentType = StringType,
      description = "UUID for the entity."
    )

  import UserType.RoleEnum

  val role: Argument[Role] =
    Argument(
      name = "role",
      argumentType = RoleEnum,
      description = "Role of the user."
    )

  def apply: ObjectType[FutureDao, Unit] =
    ObjectType(
      name = "Query",
      fields = fields[FutureDao, Unit](
        Field(
          name = "users",
          fieldType = ListType(UserType.apply),
          description = Some("Returns all users from db"),
          resolve = c => c.ctx.fetchAll()
        ),
        Field(
          name = "userById",
          fieldType = OptionType(UserType.apply),
          description = Some("Returns the user with the given identifier, if any."),
          arguments = List(id),
          resolve = c => c.ctx.fetchById(c.arg(id))
        ),
        Field(
          name = "usersByRole",
          fieldType = ListType(UserType.apply),
          description = Some("Returns all users by given role."),
          arguments = List(role),
          resolve = c => c.ctx.fetchByRole(c.arg(role))
        )
      )
    )

  def schema: Schema[FutureDao, Unit] =
    Schema(QueryType.apply)
}
