package com.example.graphQL.cats

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.testing.RecoveryApiProcess
import java.nio.file.{Files, Path}
import scala.concurrent.duration.*

final class RecoveryApiProcessSpec extends munit.CatsEffectSuite {
  private def eventually[A](read: IO[A])(done: A => Boolean): IO[A] =
    read.flatMap(value => if (done(value)) IO.pure(value) else IO.sleep(50.millis) *> eventually(read)(done))

  private def fixture: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("recovery-supervision-")))(root =>
      IO.blocking {
        val _ = Files.deleteIfExists(root.resolve("child.pid"))
        val _ = Files.deleteIfExists(root.resolve("process.lock"))
        val _ = Files.deleteIfExists(root)
        ()
      }
    )

  private def process(args: List[String]): Resource[IO, Process] =
    Resource.make(IO.blocking(new ProcessBuilder(args*).start()))(parent =>
      IO.blocking {
        parent.destroy()
        if (!parent.waitFor(7, java.util.concurrent.TimeUnit.SECONDS)) {
          val _ = parent.destroyForcibly()
          val _ = parent.waitFor()
        }
        ()
      }
    )

  private def child(root: Path, ignoreTermination: Boolean = false): List[String] = List(
    "/usr/bin/python3",
    "-c",
    "import os,signal,sys,time;" + (if (ignoreTermination) "signal.signal(signal.SIGTERM,signal.SIG_IGN);" else "") +
      "open(sys.argv[1],'w').write(str(os.getpid()));time.sleep(120)",
    root.resolve("child.pid").toString
  )

  private def handle(root: Path): IO[ProcessHandle] =
    eventually(IO.blocking {
      val marker = root.resolve("child.pid")
      if (Files.exists(marker)) Files.readString(marker).toLongOption else None
    })(_.nonEmpty).timeout(5.seconds).flatMap(pid => IO.blocking(ProcessHandle.of(pid.get).orElseThrow()))

  private def lockStatus(root: Path): IO[Int] = IO.blocking {
    new ProcessBuilder("flock", "-n", root.resolve("process.lock").toString, "true").start().waitFor()
  }

  test("a killed owner cannot leave an API alive or permit namespace cleanup during shutdown") {
    fixture.use { root =>
      val parentCode =
        "import os,subprocess,sys,time;args=sys.argv[1:];args[3]=str(os.getpid());subprocess.Popen(args);time.sleep(120)"
      val args =
        List("/usr/bin/python3", "-c", parentCode) ++ RecoveryApiProcess.command(child(root, true), 0L, Some(root))
      process(args).use { parent =>
        for {
          api <- handle(root)
          held <- lockStatus(root)
          _ = assertEquals(held, 1)
          _ <- IO.blocking { val _ = parent.destroyForcibly(); val _ = parent.waitFor(); () }
          shuttingDown <- lockStatus(root)
          _ = assertEquals(shuttingDown, 1, "cleanup must wait for the actual API, including forced termination")
          _ <- eventually(IO.blocking(api.isAlive))(alive => !alive).timeout(10.seconds)
          released <- eventually(lockStatus(root))(_ == 0).timeout(5.seconds)
          _ = assertEquals(released, 0)
        } yield ()
      }
    }
  }

  test("normal Resource release terminates and waits for the API before releasing its lock") {
    fixture.use { root =>
      for {
        api <- process(RecoveryApiProcess.command(child(root), registry = Some(root))).use(_ => handle(root))
        _ = assert(!api.isAlive)
        released <- lockStatus(root)
        _ = assertEquals(released, 0)
      } yield ()
    }
  }

  test("startup failure and owner death before launch release ownership without starting an API") {
    fixture.use { root =>
      val cases = List(
        RecoveryApiProcess.command(List("/definitely-missing-recovery-api"), registry = Some(root)),
        RecoveryApiProcess.command(child(root), owner = 0L, registry = Some(root))
      )
      cases.traverse_ { args =>
        process(args).use(parent =>
          IO.blocking {
            assert(parent.waitFor(5, java.util.concurrent.TimeUnit.SECONDS))
            assert(!Files.exists(root.resolve("child.pid")))
          }
        ) *> lockStatus(root).map(status => assertEquals(status, 0))
      }
    }
  }
}
