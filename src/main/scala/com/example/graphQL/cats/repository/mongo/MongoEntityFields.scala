package com.example.graphQL.cats.repository.mongo

/** The stored field names of a `users` document, owned once. Codecs (the allowed-field check of a decode), read
  * projections, the deleted-account validator and the search eligibility projection derive their sets from here instead
  * of repeating the names.
  */
private[mongo] object UserFields {

  /** Every field a stored user may carry; anything else fails the decode closed. */
  val stored: List[String] = List(
    MongoFields.Id,
    MongoFields.Version,
    MongoFields.Email,
    MongoFields.EmailCanonical,
    MongoFields.Name,
    MongoFields.NameCanonical,
    MongoFields.Role,
    MongoFields.Profile,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.AccountStatus,
    MongoFields.DeletedAt,
    MongoFields.AdminSingletonKey,
    MongoFields.PasswordHash,
    MongoFields.Embedding,
    MongoFields.EmbeddingMeta
  )

  /** The derived search-index state; it never advances the content revision. */
  val vector: List[String] = List(MongoFields.Embedding, MongoFields.EmbeddingMeta)

  /** What a read for display leaves out: the credential and the vector. */
  val hiddenFromProfile: List[String] = MongoFields.PasswordHash :: vector

  /** The personal data a Deleted account must not hold. */
  val erasedOnDeletion: List[String] =
    List(MongoFields.Email, MongoFields.EmailCanonical, MongoFields.PasswordHash, MongoFields.Profile) ++ vector

  /** The facts authorization needs about the acting user, and nothing else. */
  val actor: List[String] =
    List(MongoFields.Id, MongoFields.Role, MongoFields.AccountStatus, MongoFields.AdminSingletonKey)
}

/** The stored field names of a `jobs` document, owned once (see [[UserFields]]). */
private[mongo] object JobFields {

  /** Every field a stored job may carry; anything else fails the decode closed. */
  val stored: List[String] = List(
    MongoFields.Id,
    MongoFields.Version,
    MongoFields.RecruiterId,
    MongoFields.Title,
    MongoFields.Description,
    MongoFields.Requirements,
    MongoFields.Skills,
    MongoFields.Location,
    MongoFields.Status,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.ClosedAt,
    MongoFields.Embedding,
    MongoFields.EmbeddingMeta,
    MongoFields.AdmissionFence,
    MongoFields.SkillsCanonical
  )

  /** The derived search-index state; it never advances the content revision. */
  val vector: List[String] = List(MongoFields.Embedding, MongoFields.EmbeddingMeta)

  /** The fields that make a job large: the free text of its posting. */
  val posting: List[String] = List(MongoFields.Description, MongoFields.Requirements)

  /** What a read for display leaves out. */
  val hiddenFromDisplay: List[String] = vector

  /** What a summary list read leaves out in addition: the posting text. */
  val hiddenFromSummary: List[String] = vector ++ posting

  /** The ownership facts authorization needs about a job, and nothing else. */
  val owner: List[String] = List(MongoFields.Id, MongoFields.RecruiterId, MongoFields.Status)

  /** What a search eligibility read projects: the job without its vector and admission fence, with the metadata that
    * says which vector the job has.
    */
  val eligibility: List[String] =
    stored.filterNot(field => field == MongoFields.Embedding || field == MongoFields.AdmissionFence)
}
