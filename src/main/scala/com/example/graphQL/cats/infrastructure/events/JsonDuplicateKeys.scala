package com.example.graphQL.cats.infrastructure.events

import scala.annotation.tailrec

/** Detects a repeated object key at any nesting level. Parsers disagree on which duplicate wins (Circe keeps the last),
  * so the wire codec refuses such envelopes. Keys are compared after escape decoding. The input must already be valid
  * JSON; the scan is linear in its length and so bounded by the envelope budget.
  */
private[events] object JsonDuplicateKeys {

  /** `Some(keys)` is an open object with its keys so far; `None` is an open array. */
  private type Scope = Option[Set[String]]

  def present(json: String): Boolean = scan(json, 0, Nil)

  @tailrec
  private def scan(s: String, i: Int, stack: List[Scope]): Boolean =
    if (i >= s.length) false
    else
      s.charAt(i) match {
        case '{'       => scan(s, i + 1, Some(Set.empty[String]) :: stack)
        case '['       => scan(s, i + 1, None :: stack)
        case '}' | ']' => scan(s, i + 1, stack.drop(1))
        case '"'       =>
          val (text, next) = readString(s, i + 1)
          stack match {
            case Some(keys) :: rest if isKey(s, next) =>
              if (keys.contains(text)) true else scan(s, next, Some(keys + text) :: rest)
            case _ => scan(s, next, stack)
          }
        case _ => scan(s, i + 1, stack)
      }

  @tailrec
  private def isKey(s: String, i: Int): Boolean =
    i < s.length && (s.charAt(i) match {
      case ' ' | '\t' | '\n' | '\r' => isKey(s, i + 1)
      case ':'                      => true
      case _                        => false
    })

  /** Reads a string body after its opening quote; returns the decoded text and the index after the closing quote. */
  private def readString(s: String, start: Int): (String, Int) = {
    val out = new java.lang.StringBuilder
    @tailrec
    def go(i: Int): Int =
      if (i >= s.length) i
      else
        s.charAt(i) match {
          case '"'                      => i + 1
          case '\\' if i + 1 < s.length =>
            s.charAt(i + 1) match {
              case 'u' if i + 6 <= s.length =>
                out.append(Integer.parseInt(s.substring(i + 2, i + 6), 16).toChar)
                go(i + 6)
              case 'n' => out.append('\n'); go(i + 2)
              case 't' => out.append('\t'); go(i + 2)
              case 'r' => out.append('\r'); go(i + 2)
              case 'b' => out.append('\b'); go(i + 2)
              case 'f' => out.append('\f'); go(i + 2)
              case c   => out.append(c); go(i + 2)
            }
          case c => out.append(c); go(i + 1)
        }
    val end = go(start)
    (out.toString, end)
  }
}
