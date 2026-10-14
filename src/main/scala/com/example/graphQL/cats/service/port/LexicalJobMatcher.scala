package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.service.search.{JobSearchFilter, LexicalJobMatches}

/** Finds the jobs a lexical query matches in the search index. Output is index evidence only: callers must recheck
  * eligibility against operational data and never return the identities. Implementations read at most `limit + 1` hits
  * and report whether the match set was truncated.
  */
trait LexicalJobMatcher {
  def matches(text: String, filter: JobSearchFilter, limit: Int): RepositoryIO[LexicalJobMatches]
}
