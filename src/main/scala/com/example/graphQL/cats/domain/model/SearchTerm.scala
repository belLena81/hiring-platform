package com.example.graphQL.cats.domain.model

import java.util.Locale

/** The one matching form for candidate and job filter values: trimmed and lower-cased with the root locale. Stored
  * canonical sidecars (`profile.skillsCanonical`, `skillsCanonical`, `location.cityCanonical`, ...) and every filter
  * input pass through this function, so a stored value matches a request exactly when their canonical forms are equal.
  */
object SearchTerm {
  def canonical(value: String): String = value.trim.toLowerCase(Locale.ROOT)

  /** Distinct canonical forms in a deterministic order, for set-containment (`$all`) predicates. */
  def canonicalSorted(values: Iterable[String]): List[String] = values.map(canonical).toList.distinct.sorted
}
