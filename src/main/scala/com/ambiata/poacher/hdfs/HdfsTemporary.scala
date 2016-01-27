package com.ambiata.poacher.hdfs

import com.ambiata.mundane.path._
import com.ambiata.mundane.io.Temporary._
import org.apache.hadoop.conf.Configuration
import java.util.concurrent.atomic.AtomicInteger
import scalaz._, Scalaz._

case class HdfsTemporary(base: HdfsPath, seed: String) {
  private val step: AtomicInteger = new AtomicInteger(0)

  def path: Hdfs[HdfsPath] = {
    val path = setup
    val filePath = path /- java.util.UUID.randomUUID.toString
    run(s"HdfsPath($filePath.path)") >>
      Hdfs.ok(filePath)
  }

  def file: Hdfs[HdfsFile] =
    fileWithContent("")

  def fileWithContent(content: String): Hdfs[HdfsFile] = for {
    p <- path
    r <- p.write(content)
  } yield r

  def directory: Hdfs[HdfsDirectory] = {
    val path = setup
    path.mkdirsOrFail >>= (d =>
      run(s"HdfsDirectory($path.path)") >>
        Hdfs.ok(d))
  }

  def setup: HdfsPath = {
    val incr = step.incrementAndGet.toString
    val path = base /- seed /- incr
    path
  }

  def run(msg: String): Hdfs[Unit] =
    addCleanupFinalizer(msg) >>
      addPrintFinalizer(msg)

  def addCleanupFinalizer(msg: String): Hdfs[Unit] =
    if (skipCleanup) forceAddPrintFinalizer(msg)
    else             Hdfs.addFinalizer(base.delete)

  def addPrintFinalizer(msg: String): Hdfs[Unit] =
    if (print && !skipCleanup) forceAddPrintFinalizer(msg)
    else                       Hdfs.unit

  def forceAddPrintFinalizer(msg: String): Hdfs[Unit] =
    Hdfs.addFinalizer(Hdfs.putStrLn(s"Temporary: $msg"))
}

object HdfsTemporary {
  def random: HdfsTemporary =
    HdfsTemporary(hdfsTemporaryPath, java.util.UUID.randomUUID().toString)

  def hdfsTemporaryPath: HdfsPath =
    HdfsPath(Path("/tmp")) | tempUniquePath
}
