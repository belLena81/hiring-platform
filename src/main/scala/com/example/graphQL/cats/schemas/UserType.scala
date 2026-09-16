package com.example.graphQL.cats.schemas

import com.example.graphQL.cats.daos.{Dao, FutureDao}
import com.example.graphQL.cats.models.{Role, User}
import sangria.schema._
import cats.effect._
import com.example.graphQL.cats.models.Role.{Admin, Candidate, Recruiter}

object UserType {
  import sangria.macros.derive._
  //val enumType = deriveObjectType[Dao[IO], Role]
  // Create an EnumType that maps GraphQL string values to your Scala case objects
  implicit val RoleEnum: EnumType[Role] = EnumType(
    name = "Role", // The name of the type in the GraphQL Schema
    description = Some("The role of a user in the system"),
    values = List(
      EnumValue(
        name = "ADMIN", // The value in the GraphQL query (e.g., { user(role: ADMIN) })
        value = Admin,  // The Scala object that this value maps to
        description = Some("System administrator with full access")
      ),
      EnumValue(
        name = "RECRUITER",
        value = Recruiter,
        description = Some("A recruiter who manages job postings and candidates")
      ),
      EnumValue(
        name = "CANDIDATE",
        value = Candidate,
        description = Some("A candidate applying for a job")
      )
    )
  )
  def apply: ObjectType[FutureDao, User] = deriveObjectType[FutureDao, User](
    ObjectTypeName("User"),
    ObjectTypeDescription("A user of the system."),
    )
}
