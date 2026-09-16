package com.example.graphQL.cats.models

import doobie.Meta
import doobie.postgres.implicits.pgEnumString

sealed trait Role {
  def name: String
}
object Role {
  case object Admin extends Role {
    override def name: String = "Admin"
  }

  case object Recruiter extends Role {
    override def name: String = "Recruiter"
  }

  case object Candidate extends Role {
    override def name: String = "Candidate"
  }

  def toRole(role: Role): String = role.name

  def fromRole(role: String): Role =
    Option(role).collect{
      case "Admin" => Admin
      case "Recruiter" => Recruiter
      case "Candidate" => Candidate
    }.getOrElse(throw new Exception(s"Invalid role: $role"))

  implicit val roleMeta: Meta[Role] =
    pgEnumString("role", Role.fromRole, Role.toRole)
}


