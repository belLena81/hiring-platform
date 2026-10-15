package com.example.hiring.saga

import com.example.hiring.saga.BookingSagaFixture.*
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

/** SAG-03 for the engine fixture (each ported definition adds its own case later) and SAG-08.2 / rule 10. */
class SagaDefinitionWellFormednessSpec extends munit.FunSuite {
  import Phase.*

  private val sample = booking(Reserve)

  private def problems(d: Def): List[String] = SagaDefinitionChecks.problems(d, sample)

  test("SAG-03 the booking definition is well formed") {
    assertEquals(problems(standard), Nil)
  }

  test("SAG-03.1 a step without an undo declaration does not compile and the error names undo") {
    val errors = compileErrors(
      """com.example.hiring.saga.SagaStep[Int, Int, Int](name = "s", phase = 1, key = _ => "k", start = _ => Nil, lookup = _ => Nil, retryBudget = com.example.hiring.saga.RetryBudget("b"))"""
    )
    assert(errors.contains("undo"), errors)
  }

  test("SAG-03.2 a compensation is a catalogue step with its own key and budget") {
    val release = standard.steps.find(_.name == "release").get
    val commit = standard.steps.find(_.name == "commit").get
    assertEquals(commit.undo, StepUndo.Compensate(release))
    assertNotEquals(release.key(sample), commit.key(sample))
    assert(release.retryBudget.name.nonEmpty)
    val sharedKey = release.copy(key = commit.key)
    val broken = nodes.updated(Commit, nodes(Commit).copy(step = commit.copy(undo = StepUndo.Compensate(sharedKey))))
    assert(problems(definition(broken)).exists(_.contains("reuses key")), problems(definition(broken)).toString)
    val noBudget = release.copy(retryBudget = RetryBudget(""))
    val brokenBudget =
      nodes.updated(Commit, nodes(Commit).copy(step = commit.copy(undo = StepUndo.Compensate(noBudget))))
    assert(problems(definition(brokenBudget)).exists(_.contains("no retry budget")))
  }

  test("SAG-03.2 a compensation target must have its own node, be that node's step and run in a declared phase") {
    val noNode = problems(definition(nodes - Release))
    assert(noNode.exists(_.contains("compensation release has no node")), noNode.toString)
    val impostor = nodes.updated(Release, nodes(Release).copy(step = nodes(Release).step.copy(name = "other-release")))
    val notOwn = problems(definition(impostor))
    assert(notOwn.exists(_.contains("is not the step of phase")), notOwn.toString)
    val undeclared = problems(definition(phases = Phase.values.toList.filterNot(_ == Release)))
    assert(undeclared.exists(_.contains("undeclared phase")), undeclared.toString)
  }

  test("SAG-03.2 steps are equal by name and a name must denote one step") {
    val reserveStep = nodes(Reserve).step
    assertEquals(reserveStep, reserveStep.copy(key = (_: Booking) => "other"))
    assertEquals(reserveStep.hashCode, reserveStep.copy(phase = Commit).hashCode)
    val clash = nodes.updated(Commit, nodes(Commit).copy(step = nodes(Commit).step.copy(name = "reserve")))
    assert(problems(definition(clash)).exists(_.contains("step name reserve is used by different steps")))
    assertEquals(standard.steps.map(_.name).sorted, List("commit", "release", "reserve"))
  }

  test("SAG-03.3 no irreversible step has a compensation edge and no compensation is itself compensated") {
    val irreversibleWithEdge = nodes.updated(Reserve, nodes(Reserve).copy(onRejected = Rejection.Compensate()))
    assert(problems(definition(irreversibleWithEdge)).exists(_.contains("compensation edge")))
    val release = standard.steps.find(_.name == "release").get
    val cascading = release.copy(undo = StepUndo.Compensate(release.copy(name = "undo-release")))
    val commit = nodes(Commit).step.copy(undo = StepUndo.Compensate(cascading))
    val brokenCascade = nodes.updated(Commit, nodes(Commit).copy(step = commit))
    assert(problems(definition(brokenCascade)).exists(_.contains("irreversible")))
    val blankReason = nodes.updated(
      Reserve,
      nodes(Reserve).copy(step = nodes(Reserve).step.copy(undo = StepUndo.Irreversible(" ")))
    )
    assert(problems(definition(blankReason)).exists(_.contains("no reason")))
  }

  test("SAG-03.4 every non-terminal phase has a step or is declared event driven") {
    assert(problems(definition(eventDriven = Set.empty)).exists(_.startsWith("Wait")))
    assert(problems(definition(nodes - Release)).exists(_.startsWith("Release")))
    val stepOnTerminal = nodes + (Done -> nodes(Reserve))
    assert(problems(definition(stepOnTerminal)).exists(_.contains("terminal or repair phase has a step")))
  }

  test("SAG-03.4 a node bound to another phase or a lookup without a confirmation route is reported") {
    val wrongPhase = nodes.updated(Commit, nodes(Reserve))
    assert(problems(definition(wrongPhase)).exists(_.contains("is bound to phase")))
    val noRoute = nodes.updated(Commit, nodes(Commit).copy(onConfirmed = None))
    assert(problems(definition(noRoute)).exists(_.contains("without a confirmation route")))
  }

  test("SAG-03 status is derived from the phase and never stored") {
    assertEquals(
      Phase.values.toList.map(standard.status),
      List(
        SagaStatus.Running,
        SagaStatus.Running,
        SagaStatus.Compensating,
        SagaStatus.Running,
        SagaStatus.Completed,
        SagaStatus.RepairRequired
      )
    )
  }

  test("SAG-08.2 the shared saga source has no import, so it depends on nothing outside the standard library") {
    val root = Paths.get(sys.props("user.dir"), "shared", "saga", "src", "main", "scala")
    val files = Files.walk(root).iterator().asScala.filter(_.toString.endsWith(".scala")).toList
    assert(files.nonEmpty, "shared saga sources not found")
    val imports = files.flatMap(file => Files.readAllLines(file).asScala.filter(_.trim.startsWith("import ")))
    assertEquals(imports, Nil)
    val packages = files.flatMap(file => Files.readAllLines(file).asScala.filter(_.startsWith("package ")))
    assertEquals(packages.toSet, Set("package com.example.hiring.saga"))
  }
}
