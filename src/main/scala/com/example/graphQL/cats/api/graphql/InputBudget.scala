package com.example.graphQL.cats.api.graphql

import scala.annotation.tailrec
import sangria.ast

object InputBudget {
  val MaxBytes: Int = 64 * 1024
  val MaxTokens: Int = 4096
  val MaxNesting: Int = 32

  def lexical(input: String, graphql: Boolean): Boolean = {
    @tailrec
    def quoted(offset: Int, block: Boolean): Option[Int] =
      if (offset >= input.length) None
      else if (block && input.startsWith("\\\"\"\"", offset)) quoted(offset + 4, block)
      else if (block && input.startsWith("\"\"\"", offset)) Some(offset + 3)
      else if (!block && input.charAt(offset) == '\\') quoted(offset + 2, block)
      else if (!block && input.charAt(offset) == '"') Some(offset + 1)
      else quoted(offset + 1, block)

    def word(character: Char): Boolean =
      character.isLetterOrDigit || character == '_' || character == '.' || character == '-' || character == '+'

    @tailrec
    def wordEnd(offset: Int): Int =
      if (offset < input.length && word(input.charAt(offset))) wordEnd(offset + 1) else offset

    @tailrec
    def scan(offset: Int, stack: List[Char], tokens: Int): Boolean =
      if (tokens > MaxTokens || stack.size > MaxNesting) false
      else if (offset >= input.length) stack.isEmpty
      else input.charAt(offset) match {
        case character if character.isWhitespace || character == ',' => scan(offset + 1, stack, tokens)
        case '#' if graphql =>
          val newline = input.indexOf('\n', offset)
          scan(if (newline < 0) input.length else newline + 1, stack, tokens)
        case '"' =>
          val block = graphql && input.startsWith("\"\"\"", offset)
          quoted(offset + (if (block) 3 else 1), block) match {
            case Some(next) => scan(next, stack, tokens + 1)
            case None       => false
          }
        case '{' => scan(offset + 1, '}' :: stack, tokens + 1)
        case '[' => scan(offset + 1, ']' :: stack, tokens + 1)
        case '(' => scan(offset + 1, ')' :: stack, tokens + 1)
        case character @ ('}' | ']' | ')') => stack match {
          case expected :: rest if expected == character => scan(offset + 1, rest, tokens + 1)
          case _                                         => false
        }
        case character if word(character) => scan(wordEnd(offset + 1), stack, tokens + 1)
        case _                            => scan(offset + 1, stack, tokens + 1)
      }

    scan(0, Nil, 0)
  }

  def document(document: ast.Document, operationName: Option[String]): Boolean = {
    final case class Visit(selection: ast.Selection, depth: Int, fragments: Set[String])
    val operations = document.definitions.collect { case operation: ast.OperationDefinition => operation }
    val fragments = document.fragments
    val selected = operationName match {
      case Some(name) => operations.find(_.name.contains(name))
      case None      => operations.headOption.filter(_ => operations.size == 1)
    }

    @tailrec
    def walk(pending: List[Visit], visited: Int, fields: Int, aliases: Int, selected: Boolean): Boolean =
      if (visited > MaxTokens || (selected && (fields > 1000 || aliases > 32))) false
      else pending match {
        case Nil => true
        case Visit(selection, depth, path) :: rest => selection match {
          case field: ast.Field =>
            if (depth > (if (selected) 16 else MaxNesting)) false
            else walk(
              field.selections.toList.map(Visit(_, depth + 1, path)) ::: rest,
              visited + 1, fields + 1, aliases + field.alias.size, selected)
          case fragment: ast.InlineFragment =>
            walk(fragment.selections.toList.map(Visit(_, depth, path)) ::: rest,
              visited + 1, fields, aliases, selected)
          case spread: ast.FragmentSpread =>
            if (path.contains(spread.name) || path.size >= MaxNesting) false
            else fragments.get(spread.name) match {
              case Some(fragment) => walk(
                fragment.selections.toList.map(Visit(_, depth, path + spread.name)) ::: rest,
                visited + 1, fields, aliases, selected)
              case None => false
            }
        }
      }

    val definitions = document.definitions.toList.flatMap {
      case operation: ast.OperationDefinition => operation.selections.toList.map(Visit(_, 1, Set.empty))
      case fragment: ast.FragmentDefinition => fragment.selections.toList.map(Visit(_, 1, Set(fragment.name)))
      case _ => Nil
    }
    document.definitions.forall {
      case _: ast.OperationDefinition | _: ast.FragmentDefinition => true
      case _ => false
    } && selected.exists { operation =>
      walk(definitions, 0, 0, 0, false) &&
        walk(operation.selections.toList.map(Visit(_, 1, Set.empty)), 0, 0, 0, true)
    }
  }
}
