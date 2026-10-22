package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.pagination.JobProjection
import sangria.ast
import sangria.schema.Context

/** Derives how much of each job a list root must read from what the client selected. The posting text (`description`,
  * `requirements`) is the only large part of a job, so the choice has two levels: `Full` when either is selected
  * anywhere under the root (a nested selection or a fragment included, which only errs towards reading more), and
  * `Summary` otherwise. Everything authorization and the cursor need (`id`, `recruiterId`, `status`, `createdAt`) is
  * read at both levels.
  */
private[graphql] object HiringGraphQLJobProjection {
  private val PostingFields = Set("description", "requirements")

  def of(context: Context[RequestContext, Unit]): JobProjection =
    of(context.astFields, context.query.fragments)

  def of(roots: Vector[ast.Field], fragments: Map[String, ast.FragmentDefinition]): JobProjection =
    if (roots.exists(root => selectsPosting(root.selections, fragments, Set.empty))) JobProjection.Full
    else JobProjection.Summary

  private def selectsPosting(
      selections: Vector[ast.Selection],
      fragments: Map[String, ast.FragmentDefinition],
      visited: Set[String]
  ): Boolean =
    selections.exists {
      case field: ast.Field => PostingFields(field.name) || selectsPosting(field.selections, fragments, visited)
      case inline: ast.InlineFragment => selectsPosting(inline.selections, fragments, visited)
      case spread: ast.FragmentSpread =>
        // An unknown fragment cannot pass validation; reading more is the safe answer if one ever reaches here.
        !visited(spread.name) &&
        fragments
          .get(spread.name)
          .fold(true)(fragment => selectsPosting(fragment.selections, fragments, visited + spread.name))
    }
}
