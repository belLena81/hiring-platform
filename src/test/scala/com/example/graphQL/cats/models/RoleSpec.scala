package com.example.graphQL.cats.models

import com.example.graphQL.cats.models.Role.{Admin, Candidate, Recruiter}
import org.scalatest.funsuite.AnyFunSuite

final class RoleSpec extends AnyFunSuite {
  test("fromRole maps known role names") {
    assert(Role.fromRole("Admin") == Admin)
    assert(Role.fromRole("Recruiter") == Recruiter)
    assert(Role.fromRole("Candidate") == Candidate)
  }

  test("toRole returns the role name") {
    assert(Role.toRole(Admin) == "Admin")
    assert(Role.toRole(Recruiter) == "Recruiter")
    assert(Role.toRole(Candidate) == "Candidate")
  }
}
