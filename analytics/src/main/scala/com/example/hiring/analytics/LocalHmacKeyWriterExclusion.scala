package com.example.hiring.analytics

import cats.effect.IO

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

  private def run(args: Vector[String]): Result = {
    val process = new ProcessBuilder(args*).redirectErrorStream(true).start()
    val completed = process.waitFor(30L, TimeUnit.SECONDS)
    if (!completed) {
      process.destroyForcibly()
      throw AnalyticsError.InvalidConfiguration("Docker writer-exclusion probe timed out")
    }
    val output = new String(process.getInputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim
    Result(process.exitValue(), output)
  }

  private def required(args: Vector[String]): String = {
    val result = run(args)
    if (result.exitCode != 0) throw AnalyticsError.InvalidConfiguration("Docker writer-exclusion inspection failed")
    result.output
  }

  private def imageIdentity(image: String, expectedId: String, expectedUid: Int): Unit = {
    val identity = required(Vector("docker", "image", "inspect", "--format", "{{.Id}}|{{.Config.User}}", image))
    val parts = identity.split("\\|", -1).toVector
    if (parts != Vector(expectedId, expectedUid.toString))
      throw AnalyticsError.InvalidConfiguration("Docker writer image identity or nonroot UID changed")
  }

  private def probe(image: String, uid: Int, volume: String, script: String): Result =
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

  private def noRunningBindWriter(source: Path): Unit = {
    val containers = required(Vector("docker", "ps", "-q")).linesIterator.filter(_.nonEmpty).toVector
    containers.foreach { container =>
      val mounts = required(
        Vector(
          "docker",
          "inspect",
          "--format",
          "{{range .Mounts}}{{.Type}}|{{.Source}}|{{.Name}}|{{.RW}}{{println}}{{end}}",
          container
        )
      )
      mounts.linesIterator.filter(_.nonEmpty).foreach { line =>
        val fields = line.split("\\|", -1).toVector
        if (fields.size != 4 || !Set("true", "false").contains(fields(3)))
          throw AnalyticsError.InvalidConfiguration("running Docker mount inventory is malformed")
        val kind = fields(0)
        if (kind == "bind" && fields(3) == "true") {
          val path = Path.of(fields(1)).toRealPath()
          if (mountIntersectsProtectedSource(path, source))
            throw AnalyticsError.InvalidConfiguration("a running container can write the retirement bind source")
        } else if (kind == "volume" && fields(3) == "true") {
          val volumeDevice = required(
            Vector(
              "docker",
              "volume",
              "inspect",
              "--format",
              "{{if .Options}}{{index .Options \"device\"}}{{end}}",
              fields(2)
            )
          )
          val mountpoint = Path.of(fields(1)).toAbsolutePath.normalize()
          val deviceIntersects = volumeDevice.nonEmpty &&
            mountIntersectsProtectedSource(Path.of(volumeDevice).toRealPath(), source)
          if (mountIntersectsProtectedSource(mountpoint, source) || deviceIntersects)
            throw AnalyticsError.InvalidConfiguration("a running container can write the retirement volume")
        } else if (kind != "bind" && kind != "volume" && fields(3) == "true")
          throw AnalyticsError.InvalidConfiguration("a running container has an unclassified writable mount")
      }
    }
  }

  /** Checks effective ACL-aware access throughout the local fixture, then probes creation and modification rights. */
  private def hostUserCannotWrite(source: Path): Unit = {
    val uid = required(Vector("id", "-u")).toIntOption.getOrElse(
      throw AnalyticsError.InvalidConfiguration("host writer identity is unavailable")
    )
    val groups = required(Vector("id", "-G")).split("\\s+").toVector.flatMap(_.toIntOption).toSet
    if (uid == 0 || groups.isEmpty)
      throw AnalyticsError.InvalidConfiguration("host writer identity cannot prove read-only access")
    val stream = Files.walk(source)
    try {
      val entries = stream.iterator().asScala.take(100001).toVector
      if (entries.size > 100000)
        throw AnalyticsError.InvalidConfiguration("host writer permission inventory exceeds its bound")
      entries.foreach { path =>
        if (Files.isSymbolicLink(path))
          throw AnalyticsError.InvalidConfiguration("retirement volume contains a symlink")
        val owner = Files.getAttribute(path, "unix:uid").asInstanceOf[Int]
        val group = Files.getAttribute(path, "unix:gid").asInstanceOf[Int]
        val mode = Files.getAttribute(path, "unix:mode").asInstanceOf[Int]
        val writable =
          if (owner == uid) (mode & 0x80) != 0
          else if (groups.contains(group)) (mode & 0x10) != 0
          else (mode & 0x2) != 0
        if (writable)
          throw AnalyticsError.InvalidConfiguration("host user can write a retirement volume path")
        if (Files.isWritable(path))
          throw AnalyticsError.InvalidConfiguration("host user has effective write access to a retirement path")
        if (Files.isDirectory(path)) {
          val challenge = path.resolve(".hmac-host-create-" + UUID.randomUUID().toString)
          val created = try {
            Files.createFile(challenge)
            true
          } catch { case _: java.nio.file.AccessDeniedException => false }
          if (created) {
            Files.deleteIfExists(challenge)
            throw AnalyticsError.InvalidConfiguration("host user can create a retirement file")
          }
        } else if (Files.isRegularFile(path)) {
          val opened = try {
            val channel = Files.newByteChannel(path, StandardOpenOption.WRITE)
            channel.close()
            true
          } catch { case _: java.nio.file.AccessDeniedException => false }
          if (opened)
            throw AnalyticsError.InvalidConfiguration("host user can modify a retirement file")
        }
      }
    } finally stream.close()
  }

  def verify(settings: Settings, lakehouseRoot: String): IO[Unit] = IO
    .blocking {
      val valid = settings != null && settings.volumeName.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}") &&
        settings.oldImage.nonEmpty && settings.newImage.nonEmpty &&
        settings.oldImageId.matches("sha256:[0-9a-f]{64}") &&
        settings.newImageId.matches("sha256:[0-9a-f]{64}") &&
        settings.oldUid > 0 && settings.newUid > 0 && settings.oldUid != settings.newUid
      if (!valid) throw AnalyticsError.InvalidConfiguration("Docker writer-exclusion settings are invalid")

      val descriptor = required(
        Vector(
          "docker",
          "volume",
          "inspect",
          "--format",
          "{{.Driver}}|{{.Mountpoint}}|{{if .Options}}{{index .Options \"type\"}}|{{index .Options \"o\"}}|{{index .Options \"device\"}}{{else}}||{{end}}",
          settings.volumeName
        )
      ).split("\\|", -1).toVector
      if (descriptor.size != 5 || descriptor.head != "local")
        throw AnalyticsError.InvalidConfiguration("retirement volume must be a local Docker named volume")
      val mountpoint = descriptor(1)
      if (!mountpoint.startsWith("/var/lib/docker/volumes/") || !mountpoint.endsWith("/_data"))
        throw AnalyticsError.InvalidConfiguration("retirement volume is not an isolated Docker named volume")
      val source = descriptor.drop(2) match {
        case Vector("<no value>", "<no value>", "<no value>") | Vector("", "", "") =>
          Path.of(mountpoint)
        case Vector("none", "bind", device) if device.nonEmpty && Path.of(device).isAbsolute =>
          val path = Path.of(device).toAbsolutePath.normalize()
          if (
            !path.toString.contains("/.local/data/hmac-key-retirement/") ||
            !Files.isDirectory(path) || path.toRealPath() != path
          )
            throw AnalyticsError.InvalidConfiguration(
              "retirement bind source is outside the isolated local proof or is symlinked"
            )
          path
        case _ => throw AnalyticsError.InvalidConfiguration("retirement named volume options are unexpected")
      }
      val actualRoot = new URI(lakehouseRoot).normalize()
      if (
        actualRoot.getScheme != "file" || actualRoot.getAuthority != null ||
        Path.of(actualRoot).toAbsolutePath.normalize() != source.resolve("lakehouse")
      )
        throw AnalyticsError.InvalidConfiguration("retirement lakehouse root does not match the named Docker volume")
      if (
        Files.exists(source.resolve("lakehouse")) && source.resolve("lakehouse").toRealPath() !=
          source.resolve("lakehouse").toAbsolutePath.normalize()
      )
        throw AnalyticsError.InvalidConfiguration("retirement lakehouse root is symlinked")

      noRunningBindWriter(source)
      if (descriptor(2) == "none") hostUserCannotWrite(source)

      imageIdentity(settings.oldImage, settings.oldImageId, settings.oldUid)
      imageIdentity(settings.newImage, settings.newImageId, settings.newUid)
      val ownership = probe(
        settings.newImage,
        settings.newUid,
        settings.volumeName,
        "stat -c '%u:%a' /retirement-volume"
      )
      val ownerAndMode = ownership.output.split(":", -1).toVector
      val mode = ownerAndMode.lift(1).flatMap(value => scala.util.Try(Integer.parseInt(value, 8)).toOption)
      if (
        ownership.exitCode != 0 || ownerAndMode.headOption != Some(settings.newUid.toString) ||
        mode.forall(bits => (bits & 0x12) != 0)
      )
        throw AnalyticsError.InvalidConfiguration("isolated Docker volume ownership does not exclude the old writer")
      val running = required(Vector("docker", "ps", "-q", "--filter", s"volume=${settings.volumeName}"))
      if (running.nonEmpty)
        throw AnalyticsError.InvalidConfiguration("a container still has the retirement volume mounted")

      if (probe(settings.oldImage, settings.oldUid, settings.volumeName, "true").exitCode != 0)
        throw AnalyticsError.InvalidConfiguration("old writer image shell cannot be verified")
      val challenge = ".hmac-retirement-" + UUID.randomUUID().toString
      val path = "/retirement-volume/" + challenge
      if (probe(settings.oldImage, settings.oldUid, settings.volumeName, s"touch '$path'").exitCode == 0)
        throw AnalyticsError.InvalidConfiguration("old writer image can still write the retirement volume")
      if (probe(settings.newImage, settings.newUid, settings.volumeName, s"touch '$path' && rm '$path'").exitCode != 0)
        throw AnalyticsError.InvalidConfiguration("new writer image cannot write the retirement volume")
    }
    .adaptError {
      case error: AnalyticsError => error
      case NonFatal(_)           => AnalyticsError.InvalidConfiguration("Docker writer-exclusion proof is unavailable")
    }
}
