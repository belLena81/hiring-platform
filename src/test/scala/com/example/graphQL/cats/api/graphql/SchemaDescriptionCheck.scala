package com.example.graphQL.cats.api.graphql

import sangria.schema.*

/** Finds the public schema elements that have no description: every root field and argument, every input type and input
  * field, every union, and every enum with its values. Built-in introspection and Sangria scalars are skipped.
  */
object SchemaDescriptionCheck {
  private def blank(description: Option[String]): Boolean = description.forall(_.isBlank)

  def undescribed(schema: Schema[?, ?]): List[String] = {
    val roots = List(schema.query) ++ schema.mutation.toList
    val rootElements = roots.flatMap { root =>
      root.fields.flatMap { field =>
        val self = if (blank(field.description)) List(s"${root.name}.${field.name}") else Nil
        val arguments =
          field.arguments
            .filter(argument => blank(argument.description))
            .map(a => s"${root.name}.${field.name}(${a.name})")
        self ++ arguments
      }
    }
    val named = schema.allTypes.values.toList.filterNot(tpe => Schema.isBuiltInType(tpe.name))
    val typeElements = named.flatMap {
      case input: InputObjectType[?] =>
        (if (blank(input.description)) List(input.name) else Nil) ++
          input.fields.filter(field => blank(field.description)).map(field => s"${input.name}.${field.name}")
      case union: UnionType[?]   => if (blank(union.description)) List(union.name) else Nil
      case enumType: EnumType[?] =>
        (if (blank(enumType.description)) List(enumType.name) else Nil) ++
          enumType.values.filter(value => blank(value.description)).map(value => s"${enumType.name}.${value.name}")
      case scalar: ScalarType[?] => if (blank(scalar.description)) List(scalar.name) else Nil
      case _                     => Nil
    }
    (rootElements ++ typeElements).sorted
  }
}
