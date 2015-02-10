package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Temporary._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.util.concurrent.atomic.AtomicInteger
import scalaz._, Scalaz._

case class HdfsTemporary(base: LocalPath, seed: String) { //Temporary fix LocalPath should be HdfsPath
  private val step: AtomicInteger = new AtomicInteger(0)

  def path: Hdfs[Path] = {
    val incr = step.incrementAndGet.toString
    val path = new Path(new Path(base.path.path, seed), incr)
    val msg = s"Hdfs($path)"
    addCleanupFinalizer(new Path(base.path.path), msg) >>
      addPrintFinalizer(msg).as(path)
  }

  def addCleanupFinalizer(path: Path, msg: String): Hdfs[Unit] =
    if (skipCleanup) forceAddPrintFinalizer(msg)
    else             Hdfs.addFinalizer(Hdfs.deleteAll(path))

  def addPrintFinalizer(msg: String): Hdfs[Unit] =
    if (print && !skipCleanup) forceAddPrintFinalizer(msg)
    else                       Hdfs.unit

  def forceAddPrintFinalizer(msg: String): Hdfs[Unit] =
    Hdfs.addFinalizer(Hdfs.putStrLn(s"Temporary: $msg"))
}

object HdfsTemporary {
  def getTemporaryHdfsX: Hdfs[Path] = {
    val p = ??? //new Path(uniqueDirPath.path)
    addHdfsFinalizer(p).as(p)
  }

  def addHdfsFinalizer(path: Path): Hdfs[Unit] =
    Hdfs.addFinalizer(Hdfs.deleteAll(path))

  def random: HdfsTemporary =
    HdfsTemporary(uniqueLocalPath, java.util.UUID.randomUUID().toString)
}
