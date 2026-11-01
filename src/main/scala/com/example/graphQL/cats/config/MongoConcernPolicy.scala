package com.example.graphQL.cats.config

import com.mongodb.{ConnectionString, ReadConcern, ReadConcernLevel, ReadPreference, TransactionOptions, WriteConcern}

import java.util.concurrent.TimeUnit

/** The one durability policy of every application Mongo client and transaction (DDR-05). A connection string may
  * strengthen nothing and weaken nothing: a weaker one is rejected at configuration time and the client builders set
  * these values explicitly, so the effective concern never depends on a server default.
  */
object MongoConcernPolicy {

  /** Bounded by the 2 s socket read timeout of `MongoDatabaseProbe.effectiveSettings`: a replication wait that cannot
    * finish reaches the caller as a write concern error from the server (retried by the transaction runner) instead of
    * as an unclassified socket timeout, while a healthy single-member or three-member set acknowledges in milliseconds.
    */
  private val WriteTimeoutMillis = 1500L

  val writeConcern: WriteConcern =
    WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeoutMillis, TimeUnit.MILLISECONDS)
  val readConcern: ReadConcern = ReadConcern.MAJORITY
  def readPreference: ReadPreference = ReadPreference.primary()

  /** Majority reads and a majority commit give a transaction only majority-committed data to act on, and avoid the
    * snapshot-too-old failure class that a pure `snapshot` level adds for long retries.
    */
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
