package com.example.graphQL.cats.api.graphql

import cats.data.StateT
import cats.syntax.all.*
import io.circe.Json
import sangria.ast
import sangria.renderer.QueryRenderer
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.control.NonFatal

object DiagnosticPayload {
  private enum Failure { case Limit, Invalid }
  private final case class Budget(remaining: Int = 64, pending: Vector[String] = Vector.empty,
      seen: Set[String] = Set.empty, clipped: Boolean = false)
  private type Result[A] = Either[Failure, A]
  private type Capture[A] = StateT[Result, Budget, A]
  private val redacted = Json.fromString("[REDACTED]")
  private val omitted = Json.obj("omitted" -> Json.True, "truncated" -> Json.True).noSpaces
  private val credentialTerms = Vector("password", "passwd", "pwd", "token", "secret",
    "authorization", "cookie", "credential", "apikey", "privatekey", "connectionstring",
    "mongodburi", "sessionid")

  private def pure[A](value: A): Capture[A] = StateT.pure[Result, Budget, A](value)
  private def fail[A](failure: Failure): Capture[A] = StateT.liftF[Result, Budget, A](Left(failure))
  private def visit(depth: Int): Capture[Unit] = StateT { budget =>
    if (depth > 8 || budget.remaining <= 0) Left(Failure.Limit)
    else Right((budget.copy(remaining = budget.remaining - 1), ()))
  }
  private def children[A, B](values: Vector[A])(capture: A => Capture[B]): Capture[Vector[B]] =
    if (values.size > 32) fail(Failure.Limit) else values.traverse(capture)

  private def name(value: String): Capture[String] = StateT { budget =>
    val end = value.offsetByCodePoints(0, math.min(64, value.codePointCount(0, value.length)))
    Right((budget.copy(clipped = budget.clipped || end < value.length), value.substring(0, end)))
  }

  private def credential(value: String): Boolean = {
    val normalized = value.iterator.map { character =>
      if (character >= 'A' && character <= 'Z') (character + ('a' - 'A')).toChar else character
    }.filter(character => (character >= 'a' && character <= 'z') ||
      (character >= '0' && character <= '9')).mkString
    credentialTerms.exists(normalized.contains)
  }

  private def named(value: ast.NamedType, depth: Int): Capture[ast.NamedType] =
    visit(depth) *> name(value.name).map(ast.NamedType(_))

  private def variableType(value: ast.Type, depth: Int): Capture[ast.Type] = value match {
    case value: ast.NamedType => named(value, depth).map(identity[ast.Type])
    case ast.NotNullType(inner, _) => visit(depth) *> variableType(inner, depth + 1).map(ast.NotNullType(_))
    case ast.ListType(inner, _) => visit(depth) *> variableType(inner, depth + 1).map(ast.ListType(_))
  }

  private def literal(value: ast.Value, depth: Int): Capture[ast.Value] = visit(depth) *> (value match {
    case value: ast.VariableValue => name(value.name).map(ast.VariableValue(_))
    case value: ast.ListValue => children(value.values)(literal(_, depth + 1)).map(ast.ListValue(_))
    case value: ast.ObjectValue => children(value.fields) { field =>
      for {
        _ <- visit(depth + 1)
        key <- name(field.name)
        sanitized <- literal(field.value, depth + 2)
      } yield ast.ObjectField(key, sanitized)
    }.map(fields => ast.ObjectValue(fields))
    case _ => pure(ast.StringValue("[REDACTED]"))
  })

  private def arguments(values: Vector[ast.Argument], depth: Int): Capture[Vector[ast.Argument]] =
    children(values) { argument =>
      for {
        _ <- visit(depth)
        key <- name(argument.name)
        value <- literal(argument.value, depth + 1)
      } yield ast.Argument(key, value)
    }

  private def directives(values: Vector[ast.Directive], depth: Int): Capture[Vector[ast.Directive]] =
    children(values) { directive =>
      for {
        _ <- visit(depth)
        key <- name(directive.name)
        args <- arguments(directive.arguments, depth + 1)
      } yield ast.Directive(key, args)
    }

  private def variables(values: Vector[ast.VariableDefinition], depth: Int): Capture[Vector[ast.VariableDefinition]] =
    children(values) { variable =>
      for {
        _ <- visit(depth)
        key <- name(variable.name)
        tpe <- variableType(variable.tpe, depth + 1)
        default <- variable.defaultValue.traverse(literal(_, depth + 1))
        dirs <- directives(variable.directives, depth + 1)
      } yield ast.VariableDefinition(key, tpe, default, dirs)
    }

  private def selections(values: Vector[ast.Selection], depth: Int): Capture[Vector[ast.Selection]] =
    children(values) { selection =>
      visit(depth) *> (selection match {
        case field: ast.Field => for {
          alias <- field.alias.traverse(name)
          key <- name(field.name)
          args <- arguments(field.arguments, depth + 1)
          dirs <- directives(field.directives, depth + 1)
          nested <- selections(field.selections, depth + 1)
        } yield ast.Field(alias, key, args, dirs, nested)
        case fragment: ast.InlineFragment => for {
          condition <- fragment.typeCondition.traverse(named(_, depth + 1))
          dirs <- directives(fragment.directives, depth + 1)
          nested <- selections(fragment.selections, depth + 1)
        } yield ast.InlineFragment(condition, dirs, nested)
        case spread: ast.FragmentSpread => for {
          key <- name(spread.name)
          dirs <- directives(spread.directives, depth + 1)
          _ <- StateT.modify[Result, Budget](budget =>
            budget.copy(pending = budget.pending :+ spread.name))
        } yield ast.FragmentSpread(key, dirs)
      })
    }

  private def operation(value: ast.OperationDefinition): Capture[ast.OperationDefinition] = for {
    _ <- visit(1)
    key <- value.name.traverse(name)
    vars <- variables(value.variables, 2)
    dirs <- directives(value.directives, 2)
    fields <- selections(value.selections, 2)
  } yield ast.OperationDefinition(value.operationType, key, vars, dirs, fields)

  private def fragments(available: Map[String, ast.FragmentDefinition]): Capture[Vector[ast.FragmentDefinition]] =
    StateT.get[Result, Budget].flatMap { budget =>
      budget.pending.headOption match {
        case None => pure(Vector.empty)
        case Some(key) =>
          val advance = StateT.modify[Result, Budget](current =>
            current.copy(pending = current.pending.tail, seen = current.seen + key))
          if (budget.seen(key)) advance *> fragments(available)
          else available.get(key) match {
            case None => fail(Failure.Invalid)
            case Some(fragment) => for {
              _ <- advance
              _ <- visit(1)
              sanitizedName <- name(fragment.name)
              condition <- named(fragment.typeCondition, 2)
              dirs <- directives(fragment.directives, 2)
              vars <- variables(fragment.variables, 2)
              fields <- selections(fragment.selections, 2)
              rest <- fragments(available)
            } yield ast.FragmentDefinition(sanitizedName, condition, dirs, fields, vars) +: rest
          }
      }
    }

  private def jsonValue(value: Json, depth: Int, booleanAllowed: Boolean = false): Capture[Json] =
    visit(depth) *> (value.asObject match {
      case Some(fields) =>
        if (fields.size > 32) fail(Failure.Limit)
        else children(fields.toVector) { case (key, nested) =>
          for {
            sanitizedName <- name(key)
            sanitized <- if (credential(key)) visit(depth + 1).as(redacted)
              else jsonValue(nested, depth + 1)
          } yield sanitizedName -> sanitized
        }.map(Json.fromFields)
      case None => value.asArray match {
        case Some(values) => children(values)(jsonValue(_, depth + 1)).map(Json.fromValues)
        case None => pure(if (booleanAllowed && value.isBoolean) value else redacted)
      }
    })

  private def jsonVariables(value: Json, allowed: Set[String]): Capture[Json] = value.asObject match {
    case None => fail(Failure.Invalid)
    case Some(fields) =>
      if (fields.size > 32) fail(Failure.Limit)
      else visit(1) *> children(fields.toVector) { case (key, nested) =>
        for {
          sanitizedName <- name(key)
          sanitized <- if (credential(key)) visit(2).as(redacted)
            else jsonValue(nested, 2, allowed(key))
        } yield sanitizedName -> sanitized
      }.map(Json.fromFields)
  }

  private def booleanType(value: ast.Type): Boolean = value match {
    case ast.NamedType("Boolean", _) | ast.NotNullType(ast.NamedType("Boolean", _), _) => true
    case _ => false
  }

  def capture(request: GraphQLRequest): Option[String] = try {
    if (request.document.definitions.size > 32) Some(omitted)
    else {
      val definitions = request.document.definitions
      val operations = definitions.collect { case value: ast.OperationDefinition => value }
      val candidates = request.operationName match {
        case Some(key) => operations.filter(_.name.contains(key))
        case None => operations
      }
      candidates.headOption.filter(_ => candidates.size == 1).flatMap { selected =>
        val available = definitions.collect { case value: ast.FragmentDefinition => value.name -> value }.toMap
        val sanitized = for {
          query <- operation(selected)
          reachable <- fragments(available)
          values <- jsonVariables(request.variables,
            selected.variables.filter(variable => booleanType(variable.tpe)).map(_.name).toSet)
        } yield (query, reachable, values)
        sanitized.run(Budget()).fold(
          {
            case Failure.Limit => Some(omitted)
            case Failure.Invalid => None
          },
          { case (budget, (query, reachable, values)) =>
            val payload = Json.obj(
              "query" -> Json.fromString(QueryRenderer.render(ast.Document(Vector(query) ++ reachable), QueryRenderer.Compact)),
              "variables" -> values,
              "operationName" -> query.name.fold(Json.Null)(Json.fromString),
              "truncated" -> Json.fromBoolean(budget.clipped)).noSpaces
            Some(if (payload.getBytes(UTF_8).length <= 2048) payload else omitted)
          })
      }
    }
  } catch { case NonFatal(_) => None }
}
