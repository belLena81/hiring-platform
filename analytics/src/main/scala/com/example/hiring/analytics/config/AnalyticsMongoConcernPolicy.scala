package com.example.hiring.analytics.config

import com.mongodb.{ConnectionString, ReadConcern, ReadConcernLevel, ReadPreference, TransactionOptions, WriteConcern}

import java.util.concurrent.TimeUnit

/** The one durability policy of every analytics Mongo client and transaction (DDR-05). Mirrors the application's
  * `com.example.graphQL.cats.config.MongoConcernPolicy`, which lives in the separate root build; keep the accepted URI
  * rules identical. A connection string may weaken nothing: a weaker one is rejected at configuration time, and the
  * client builder and every transaction set these values explicitly, so the effective concern never depends on a server
  * default.
  *
  * The analytics build runs no `$search`, `$vectorSearch` or `$listSearchIndexes` stage (these require read concern
  * `local` on the application side), so no handle needs a weaker read concern than `majority`.
  */
private[analytics] object AnalyticsMongoConcernPolicy {

  /** The same 15 s as the per-collection pins (lakehouse lock, streaming registry, activation gate, replay journal):
    * the analytics client sets no socket read timeout, so a replication wait ends here as a server write concern error
    * (labelled for the transaction retry) rather than hanging, while a healthy set acknowledges in milliseconds.
    */
  val WriteTimeoutMillis: Long = 15000L

  val writeConcern: WriteConcern =
    WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeoutMillis, TimeUnit.MILLISECONDS)
  val readConcern: ReadConcern = ReadConcern.MAJORITY
  def readPreference: ReadPreference = ReadPreference.primary()

  val transactionOptions: TransactionOptions =
    TransactionOptions
      .builder()
      .writeConcern(writeConcern)
      .readConcern(readConcern)
      .readPreference(readPreference)
      .build()

  private val AcceptedReadConcernLevels =
    Set(ReadConcernLevel.LOCAL, ReadConcernLevel.MAJORITY, ReadConcernLevel.SNAPSHOT)

  /** True when the URI states no write, journal, read preference or read concern weaker than the policy. A numeric or
    * tagged `w` is rejected because its strength depends on the replica-set size, which a URI cannot show.
    */
  def accepts(uri: ConnectionString): Boolean =
    Option(uri.getWriteConcern).forall { concern =>
      Option(concern.getWObject).forall(_ == WriteConcern.MAJORITY.getWString) &&
      !Option(concern.getJournal).contains(java.lang.Boolean.FALSE)
    } &&
      Option(uri.getReadPreference).forall(_ == ReadPreference.primary()) &&
      Option(uri.getReadConcern).flatMap(concern => Option(concern.getLevel)).forall(AcceptedReadConcernLevels.contains)
}
