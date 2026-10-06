package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.EmbeddingMeta
import com.example.graphQL.cats.shared.crypto.SourceHash

/** Pure preparation shared by job and candidate document embedding. */
enum EmbeddingPreparation {
  case NoWork, DocumentTooLarge
  case Prepared(text: String, sourceHash: String)
}

object EmbeddingPreparation {
  def prepare(
      source: Option[String],
      existing: Option[EmbeddingMeta],
      model: String,
      maximumChars: Int
  ): EmbeddingPreparation =
    source.fold[EmbeddingPreparation](NoWork) { text =>
      val hash = SourceHash.sha256(text)
      // Preserve current metadata's precedence over size rejection during replay.
      if (existing.exists(meta => meta.model == model && meta.sourceHash == hash)) NoWork
      else if (text.length > maximumChars) DocumentTooLarge
      else Prepared(text, hash)
    }
}
