package com.example.graphQL.cats.models

import com.example.graphQL.cats.models.Role.{Admin, Candidate, Recruiter}
import munit.FunSuite

final class RoleSpec extends FunSuite {
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
