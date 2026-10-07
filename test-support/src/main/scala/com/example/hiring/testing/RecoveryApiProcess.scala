package com.example.hiring.testing

/** Test-only supervision: terminate the API even when its owning test JVM cannot run finalizers. */
object RecoveryApiProcess {
  private val supervisor = """import fcntl,os,signal,subprocess,sys,time
owner=int(sys.argv[1])
lock=None
if sys.argv[2]!="-":
    lock=open(sys.argv[2],"a")
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
stopping=False
def stop(signum,frame):
    global stopping
    stopping=True
signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
child=None
try:
    if os.getppid()!=owner: sys.exit(1)
    child=subprocess.Popen(sys.argv[3:])
    while child.poll() is None and not stopping and os.getppid()==owner:
        time.sleep(0.05)
finally:
    if child is not None and child.poll() is None:
        child.terminate()
        try: child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
    if lock is not None: lock.close()
"""

  def command(
      child: List[String],
      owner: Long = ProcessHandle.current().pid(),
      registry: Option[java.nio.file.Path] = sys.env.get("HIRING_TEST_RUN_REGISTRY").map(java.nio.file.Path.of(_))
  ): List[String] =
    List(
      "/usr/bin/python3",
      "-c",
      supervisor,
      owner.toString,
      registry.fold("-")(_.resolve("process.lock").toString)
    ) ++ child
}
