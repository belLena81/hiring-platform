package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Indexes, Projections, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** `028_binary_embedding_vectors`: `users.embedding` and `jobs.embedding` are stored as BSON binary vectors.
  *
  * The domain type of a vector element is `Float`, but the vectors were stored as arrays of float64 (8 bytes per
  * element plus array framing). A float32 binary vector (binData subtype 9) is exact for every value the application
  * wrote and about half the size, which also shrinks Atlas Vector Search input. The vector index definition keeps its
  * path, `numDimensions` and `similarity`; mongot indexes the binary vector in place of the array.
  *
  * Each target (users, then jobs) is scanned in bounded `_id` batches whose pending predicate is `embedding` of BSON
  * type array, with the ledger checkpoint `<target ordinal>:<last _id>`. A row is converted by one `updateOne` guarded
  * by `_id` and by the exact array that was read, so a concurrent embedding write (which stores a binary vector itself)
  * is never overwritten and a replay or a second process is a no-op. A non-numeric or non-finite element fails the step
  * closed naming only the collection. The embedding write does not touch `version`, so the conversion leaves content
  * revisions alone. After the first full scan one further scan from the start runs without moving the checkpoint when a
  * pending row remains, before the step fails.
  *
  * Then each target is audited with `$nor` against its successor validator (`userValidator` and `jobValidator`: a
  * present `embedding` must be `binData`), the validators are installed, and the audit repeats to close the window
  * between the audit and the install.
  *
  * The completed proof is read-only and does not scan: both successor validators must be installed unchanged, and the
  * strict validators reject an array `embedding` on any later write, so a regression requires a writer that bypasses
  * validation. Validator drift fails closed naming the collection and is never overwritten.
  *
  * Recovery is forward-only. Stop every writer running an earlier binary before upgrading: it writes float64 arrays,
  * which the successor validators reject, and it cannot read binary vectors. Application rollback does not convert the
  * vectors back; repair forward with the current binary (the embedding worker can also regenerate any vector).
  */
private[mongo] object MongoBinaryEmbeddingVectorMigrations {
  private val Id: MigrationId = MigrationIds.BinaryEmbeddingVectors
  val MigrationId: String = Id.value

  private final case class Target(collection: String, validator: Document, audit: Document) {
    val pending: Bson = Filters.`type`(MongoFields.Embedding, "array")
  }

  /** The position is the checkpoint ordinal: never reorder, and only append. */
  private val targets: Vector[Target] = Vector(
    Target(MongoCollections.Users, MongoHiringValidators.userValidator, MongoHiringValidators.userValidator),
    Target(MongoCollections.Jobs, MongoHiringValidators.jobValidator, MongoHiringValidators.jobBinaryVectorAudit)
  )

  private val CheckpointPattern = raw"(\d{2}):(.+)".r

  private def checkpointOf(ordinal: Int, id: String): String = f"$ordinal%02d:$id"

  private def key(run: MigrationRun, row: Document): IO[String] =
    Option(row.get(MongoFields.Id)).collect { case value: String if value.nonEmpty => value } match {
      case Some(value) => IO.pure(value)
      case None        => run.fail[String]("found an invalid document key")
    }

  private def page(run: MigrationRun, target: Target, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      run.database
        .getCollection(target.collection)
        .find(after.fold(target.pending)(id => Filters.and(target.pending, Filters.gt(MongoFields.Id, id))))
        .projection(Projections.include(MongoFields.Id, MongoFields.Embedding))
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  private def resume(run: MigrationRun): IO[Option[(Int, String)]] =
    run.textCheckpoint.flatMap {
      case None                                 => IO.pure(None)
      case Some(CheckpointPattern(ordinal, id)) =>
        val index = ordinal.toInt
        if (index < targets.size) IO.pure(Some(index -> id)) else run.fail("unsupported checkpoint")
      case Some(_) => run.fail("unsupported checkpoint")
    }

  /** The float32 elements of a stored float64 array, or `None` for an element that is not a finite number. */
  private[mongo] def floats(stored: java.util.List[?]): Option[List[Float]] =
    stored.asScala.toList.traverse {
      // A finite double beyond the float32 range would narrow to Infinity: fail closed instead.
      case value: Number if java.lang.Float.isFinite(value.floatValue()) && value.doubleValue().isFinite =>
        Some(value.floatValue())
      case _ => None
    }

  private def convert(run: MigrationRun, target: Target, row: Document): IO[Unit] =
    key(run, row).flatMap { id =>
      Option(row.get(MongoFields.Embedding)) match {
        case Some(stored: java.util.List[?]) =>
          floats(stored) match {
            case Some(values) =>
              run.database
                .getCollection(target.collection)
                .updateOne(
                  Filters.and(Filters.eq(MongoFields.Id, id), Filters.eq(MongoFields.Embedding, stored)),
                  Updates.set(MongoFields.Embedding, MongoEmbeddingVectors.encode(values))
                )
                .void
            case None => run.fail(s"found a malformed vector in ${target.collection}")
          }
        case _ => IO.unit
      }
    }

  private def scan(run: MigrationRun, from: Option[(Int, String)], checkpoint: Boolean): IO[Unit] =
    targets.zipWithIndex.traverse_ { case (target, ordinal) =>
      val skipped = from.exists { case (start, _) => ordinal < start }
      val after = from.collect { case (start, id) if start == ordinal => id }
      IO.unlessA(skipped)(
        MongoBatchScan.byId(MongoBatchScan.BatchSize, after)(page(run, target, _))(key(run, _)) { rows =>
          rows.traverse_(convert(run, target, _)) *>
            IO.whenA(checkpoint)(
              rows.lastOption.traverse_(key(run, _).flatMap(id => run.advance(checkpointOf(ordinal, id))))
            )
        }
      )
    }

  private def pendingTarget(database: MongoHiringSetup.SetupDatabase): IO[Option[Target]] =
    targets.toList.findM(target =>
      database.getCollection(target.collection).find(target.pending).limit(1).first.map(_.nonEmpty)
    )

  /** Fails with the first collection holding a row its successor validator rejects; the error names only it. */
  private def audit(run: MigrationRun): IO[Unit] =
    targets.traverse_ { target =>
      run.database
        .getCollection(target.collection)
        .find(new Document("$nor", List(target.audit).asJava))
        .limit(1)
        .first
        .flatMap(invalid =>
          IO.whenA(invalid.nonEmpty)(run.fail[Unit](s"found invalid stored data in ${target.collection}"))
        )
    }

  private def convertAll(run: MigrationRun): IO[Unit] =
    resume(run).flatMap(scan(run, _, checkpoint = true)) *>
      pendingTarget(run.database).flatMap {
        case None    => IO.unit
        case Some(_) =>
          scan(run, None, checkpoint = false) *> pendingTarget(run.database).flatMap {
            case None         => IO.unit
            case Some(target) => run.fail(s"rows to convert remain in ${target.collection}")
          }
      }

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, MigrationIds.JobCanonicalFilters)
      _ <- convertAll(run)
      _ <- audit(run)
      _ <- targets.traverse_(target => MongoHiringValidators.install(run.database, target.collection, target.validator))
      _ <- audit(run)
    } yield ()

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators
      .assertStrictValidators(database, targets.toList.map(target => target.collection -> target.validator))
      .as(CompletedProof.Trusted)

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
