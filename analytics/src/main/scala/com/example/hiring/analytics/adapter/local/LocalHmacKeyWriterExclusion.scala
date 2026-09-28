package com.example.hiring.analytics.adapter.local
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.Async
import cats.syntax.all.*

import java.util.UUID
import java.net.URI
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Host-only proof for the isolated Docker volume used by the local key-rotation fixture. Host root and Docker
  * administrators are trusted: either can override storage permissions and replace containers.
  */
private[analytics] object LocalHmacKeyWriterExclusion {
  final case class Settings(
      volumeName: String,
      oldImage: String,
      oldImageId: String,
      oldUid: Int,
      newImage: String,
      newImageId: String,
      newUid: Int
  )

  private final case class Result(exitCode: Int, output: String)

  private def run(args: Vector[String]): Either[AnalyticsError, Result] = {
    val process = new ProcessBuilder(args*).redirectErrorStream(true).start()
    val completed = process.waitFor(30L, TimeUnit.SECONDS)
    if (!completed) {
      process.destroyForcibly()
      Left(AnalyticsError.InvalidConfiguration("Docker writer-exclusion probe timed out"))
    } else {
      val output = new String(process.getInputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim
      Right(Result(process.exitValue(), output))
    }
  }

  private def required(args: Vector[String]): Either[AnalyticsError, String] =
    run(args).flatMap(result =>
      Either.cond(
        result.exitCode == 0,
        result.output,
        AnalyticsError.InvalidConfiguration("Docker writer-exclusion inspection failed")
      )
    )

  private def imageIdentity(image: String, expectedId: String, expectedUid: Int): Either[AnalyticsError, Unit] =
    required(Vector("docker", "image", "inspect", "--format", "{{.Id}}|{{.Config.User}}", image)).flatMap { identity =>
      Either.cond(
        identity.split("\\|", -1).toVector == Vector(expectedId, expectedUid.toString),
        (),
        AnalyticsError.InvalidConfiguration("Docker writer image identity or nonroot UID changed")
      )
    }

  private def probe(image: String, uid: Int, volume: String, script: String): Either[AnalyticsError, Result] =
    run(
      Vector(
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "--user",
        s"$uid:$uid",
        "--mount",
        s"type=volume,src=$volume,dst=/retirement-volume",
        "--entrypoint",
        "/bin/sh",
        image,
        "-c",
        script
      )
    )

  /** A bind mount of the root, any ancestor, or any descendant can write retirement files. */
  private[analytics] def mountIntersectsProtectedSource(mountSource: Path, protectedSource: Path): Boolean = {
    val mount = mountSource.toAbsolutePath.normalize()
    val protectedPath = protectedSource.toAbsolutePath.normalize()
    mount.startsWith(protectedPath) || protectedPath.startsWith(mount)
  }

  private def noRunningBindWriter(source: Path): Either[AnalyticsError, Unit] =
    required(Vector("docker", "ps", "-q")).flatMap { listing =>
      listing.linesIterator.filter(_.nonEmpty).toVector.traverse_ { container =>
        required(
          Vector(
            "docker",
            "inspect",
            "--format",
            "{{range .Mounts}}{{.Type}}|{{.Source}}|{{.Name}}|{{.RW}}{{println}}{{end}}",
            container
          )
        ).flatMap { mounts =>
          mounts.linesIterator.filter(_.nonEmpty).toVector.traverse_ { line =>
            val fields = line.split("\\|", -1).toVector
            if (fields.size != 4 || !Set("true", "false").contains(fields(3)))
              Left(AnalyticsError.InvalidConfiguration("running Docker mount inventory is malformed"))
            else if (fields(0) == "bind" && fields(3) == "true") {
              val path = Path.of(fields(1)).toRealPath()
              Either.cond(
                !mountIntersectsProtectedSource(path, source),
                (),
                AnalyticsError.InvalidConfiguration("a running container can write the retirement bind source")
              )
            } else if (fields(0) == "volume" && fields(3) == "true")
              required(
                Vector(
                  "docker",
                  "volume",
                  "inspect",
                  "--format",
                  "{{if .Options}}{{index .Options \"device\"}}{{end}}",
                  fields(2)
                )
              ).flatMap { volumeDevice =>
                val mountpoint = Path.of(fields(1)).toAbsolutePath.normalize()
                val deviceIntersects = volumeDevice.nonEmpty &&
                  mountIntersectsProtectedSource(Path.of(volumeDevice).toRealPath(), source)
                Either.cond(
                  !mountIntersectsProtectedSource(mountpoint, source) && !deviceIntersects,
                  (),
                  AnalyticsError.InvalidConfiguration("a running container can write the retirement volume")
                )
              }
            else
              Either.cond(
                fields(0) == "bind" || fields(0) == "volume" || fields(3) == "false",
                (),
                AnalyticsError.InvalidConfiguration("a running container has an unclassified writable mount")
              )
          }
        }
      }
    }

  /** Checks effective ACL-aware access throughout the local fixture, then probes creation and modification rights. */
  private def hostUserCannotWrite(source: Path): Either[AnalyticsError, Unit] =
    for {
      uidText <- required(Vector("id", "-u"))
      uid <- uidText.toIntOption.toRight(AnalyticsError.InvalidConfiguration("host writer identity is unavailable"))
      groupText <- required(Vector("id", "-G"))
      groups = groupText.split("\\s+").toVector.flatMap(_.toIntOption).toSet
      _ <- Either.cond(
        uid != 0 && groups.nonEmpty,
        (),
        AnalyticsError.InvalidConfiguration("host writer identity cannot prove read-only access")
      )
      _ <- {
        val stream = Files.walk(source)
        try {
          val entries = stream.iterator().asScala.take(100001).toVector
          if (entries.size > 100000)
            Left(AnalyticsError.InvalidConfiguration("host writer permission inventory exceeds its bound"))
          else
            entries.traverse_ { path =>
              if (Files.isSymbolicLink(path))
                Left(AnalyticsError.InvalidConfiguration("retirement volume contains a symlink"))
              else {
                val owner = Files.getAttribute(path, "unix:uid").asInstanceOf[Int]
                val group = Files.getAttribute(path, "unix:gid").asInstanceOf[Int]
                val mode = Files.getAttribute(path, "unix:mode").asInstanceOf[Int]
                val writable =
                  if (owner == uid) (mode & 0x80) != 0
                  else if (groups.contains(group)) (mode & 0x10) != 0
                  else (mode & 0x2) != 0
                val permissionCheck =
                  if (writable)
                    Left(AnalyticsError.InvalidConfiguration("host user can write a retirement volume path"))
                  else if (Files.isWritable(path))
                    Left(
                      AnalyticsError.InvalidConfiguration("host user has effective write access to a retirement path")
                    )
                  else if (Files.isDirectory(path)) {
                    val challenge = path.resolve(".hmac-host-create-" + UUID.randomUUID().toString)
                    val created = try {
                      Files.createFile(challenge)
                      true
                    } catch { case _: java.nio.file.AccessDeniedException => false }
                    if (created) Files.deleteIfExists(challenge)
                    Either.cond(
                      !created,
                      (),
                      AnalyticsError.InvalidConfiguration("host user can create a retirement file")
                    )
                  } else if (Files.isRegularFile(path)) {
                    val opened = try {
                      val channel = Files.newByteChannel(path, StandardOpenOption.WRITE)
                      channel.close()
                      true
                    } catch { case _: java.nio.file.AccessDeniedException => false }
                    Either.cond(
                      !opened,
                      (),
                      AnalyticsError.InvalidConfiguration("host user can modify a retirement file")
                    )
                  } else Right(())
                permissionCheck
              }
            }
        } finally stream.close()
      }
    } yield ()

  private def verifyBlocking(settings: Settings, lakehouseRoot: String): Either[AnalyticsError, Unit] = {
    val validSettings = settings.volumeName.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}") &&
      settings.oldImage.nonEmpty && settings.newImage.nonEmpty &&
      settings.oldImageId.matches("sha256:[0-9a-f]{64}") &&
      settings.newImageId.matches("sha256:[0-9a-f]{64}") &&
      settings.oldUid > 0 && settings.newUid > 0 && settings.oldUid != settings.newUid
    for {
      _ <- Either.cond(
        validSettings,
        (),
        AnalyticsError.InvalidConfiguration("Docker writer-exclusion settings are invalid")
      )
      descriptorText <- required(
        Vector(
          "docker",
          "volume",
          "inspect",
          "--format",
          "{{.Driver}}|{{.Mountpoint}}|{{if .Options}}{{index .Options \"type\"}}|{{index .Options \"o\"}}|{{index .Options \"device\"}}{{else}}||{{end}}",
          settings.volumeName
        )
      )
      descriptor = descriptorText.split("\\|", -1).toVector
      _ <- Either.cond(
        descriptor.size == 5 && descriptor.headOption.contains("local"),
        (),
        AnalyticsError.InvalidConfiguration("retirement volume must be a local Docker named volume")
      )
      mountpoint = descriptor(1)
      _ <- Either.cond(
        mountpoint.startsWith("/var/lib/docker/volumes/") && mountpoint.endsWith("/_data"),
        (),
        AnalyticsError.InvalidConfiguration("retirement volume is not an isolated Docker named volume")
      )
      source <- descriptor.drop(2) match {
        case Vector("<no value>", "<no value>", "<no value>") | Vector("", "", "") => Right(Path.of(mountpoint))
        case Vector("none", "bind", device) if device.nonEmpty && Path.of(device).isAbsolute =>
          val path = Path.of(device).toAbsolutePath.normalize()
          Either.cond(
            path.toString.contains("/.local/data/hmac-key-retirement/") &&
              Files.isDirectory(path) && path.toRealPath() == path,
            path,
            AnalyticsError.InvalidConfiguration(
              "retirement bind source is outside the isolated local proof or is symlinked"
            )
          )
        case _ => Left(AnalyticsError.InvalidConfiguration("retirement named volume options are unexpected"))
      }
      actualRoot = new URI(lakehouseRoot).normalize()
      _ <- Either.cond(
        actualRoot.getScheme == "file" && actualRoot.getAuthority == null &&
          Path.of(actualRoot).toAbsolutePath.normalize() == source.resolve("lakehouse"),
        (),
        AnalyticsError.InvalidConfiguration("retirement lakehouse root does not match the named Docker volume")
      )
      _ <- Either.cond(
        !Files.exists(source.resolve("lakehouse")) ||
          source.resolve("lakehouse").toRealPath() == source.resolve("lakehouse").toAbsolutePath.normalize(),
        (),
        AnalyticsError.InvalidConfiguration("retirement lakehouse root is symlinked")
      )
      _ <- noRunningBindWriter(source)
      _ <- if (descriptor(2) == "none") hostUserCannotWrite(source) else Right(())
      _ <- imageIdentity(settings.oldImage, settings.oldImageId, settings.oldUid)
      _ <- imageIdentity(settings.newImage, settings.newImageId, settings.newUid)
      ownership <- probe(settings.newImage, settings.newUid, settings.volumeName, "stat -c '%u:%a' /retirement-volume")
      ownerAndMode = ownership.output.split(":", -1).toVector
      mode = ownerAndMode.lift(1).flatMap(value => scala.util.Try(Integer.parseInt(value, 8)).toOption)
      _ <- Either.cond(
        ownership.exitCode == 0 && ownerAndMode.headOption.contains(settings.newUid.toString) &&
          mode.exists(bits => (bits & 0x12) == 0),
        (),
        AnalyticsError.InvalidConfiguration("isolated Docker volume ownership does not exclude the old writer")
      )
      running <- required(Vector("docker", "ps", "-q", "--filter", s"volume=${settings.volumeName}"))
      _ <- Either.cond(
        running.isEmpty,
        (),
        AnalyticsError.InvalidConfiguration("a container still has the retirement volume mounted")
      )
      shell <- probe(settings.oldImage, settings.oldUid, settings.volumeName, "true")
      _ <- Either.cond(
        shell.exitCode == 0,
        (),
        AnalyticsError.InvalidConfiguration("old writer image shell cannot be verified")
      )
      challenge = ".hmac-retirement-" + UUID.randomUUID().toString
      path = "/retirement-volume/" + challenge
      oldWrite <- probe(settings.oldImage, settings.oldUid, settings.volumeName, s"touch '$path'")
      _ <- Either.cond(
        oldWrite.exitCode != 0,
        (),
        AnalyticsError.InvalidConfiguration("old writer image can still write the retirement volume")
      )
      newWrite <- probe(settings.newImage, settings.newUid, settings.volumeName, s"touch '$path' && rm '$path'")
      _ <- Either.cond(
        newWrite.exitCode == 0,
        (),
        AnalyticsError.InvalidConfiguration("new writer image cannot write the retirement volume")
      )
    } yield ()
  }

  def verify[F[_]: Async](settings: Settings, lakehouseRoot: String): F[Unit] = Async[F]
    .blocking(verifyBlocking(settings, lakehouseRoot))
    .flatMap(Async[F].fromEither)
    .adaptError {
      case error: AnalyticsError => error
      case NonFatal(_)           => AnalyticsError.InvalidConfiguration("Docker writer-exclusion proof is unavailable")
    }
}
