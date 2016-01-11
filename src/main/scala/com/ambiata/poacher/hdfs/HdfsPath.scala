package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.data._
import com.ambiata.mundane.io._
import com.ambiata.mundane.path._

import java.io._
import java.net.URI

import org.apache.hadoop.fs.FileSystem

import scala.io.Codec
import scalaz._, Scalaz._, effect.Effect._
import MemoryConversions._

/**
 * 'HdfsPath' is an unknown hdfs location which means that
 * either nothing exists at that location or that possibly
 * something exists and we just don't know yet. A file that
 * is known to exist is denoted by either 'HdfsFile' or
 * 'HdfsDirectory'.
 */
case class HdfsPath(path: Path) {
  def /(other: Path): HdfsPath =
    HdfsPath(path / other)

  def join(other: Path): HdfsPath =
    /(other)

  def |(other: Component): HdfsPath =
    HdfsPath(path | other)

  def extend(other: Component): HdfsPath =
    |(other)

  def /-(other: String): HdfsPath =
    HdfsPath(path /- other)

  def rebaseTo(other: HdfsPath): Option[HdfsPath] =
    path.rebaseTo(other.path).map(HdfsPath(_))

  def toHPath: HPath =
    new HPath(path.path)

  def dirname: HdfsPath =
    HdfsPath(path.dirname)

  def basename: Option[Component] =
    path.basename

  def exists: Hdfs[Boolean] =
    determine.map(_.isDefined)

  def onExists[A](success: => Hdfs[A], missing: => Hdfs[A]): Hdfs[A] =
    exists >>= (e =>
      if (e) success
      else   missing
    )

  def whenExists(thunk: => Hdfs[Unit]): Hdfs[Unit] =
    onExists(thunk, Hdfs.unit)

  def doesExist[A](error: String, thunk: => Hdfs[A]): Hdfs[A] =
    onExists(thunk, Hdfs.fail(error))

  def doesNotExist[A](error: String, thunk: => Hdfs[A]): Hdfs[A] =
    onExists(Hdfs.fail(error), thunk)

  def delete: Hdfs[Unit] =
    determinef(_.delete, _.delete)

  def isDirectory: Hdfs[Boolean] =
    determinefWithPure(_ => false, _ => true, false)

  def determine: Hdfs[Option[HdfsFile \/ HdfsDirectory]] = for {
    f <- Hdfs.filesystem
    e <- Hdfs.safe(f.exists(toHPath))
    o <- if (e)
           Hdfs.safe[Boolean](f.isFile(toHPath)) >>= (z => (
             if (z) HdfsFile.unsafe(path.path).left
             else   HdfsDirectory.unsafe(path.path).right
           ).some.pure[Hdfs])
         else
           none.pure[Hdfs]
  } yield o

  def determinef[A](file: HdfsFile => Hdfs[A], directory: HdfsDirectory => Hdfs[A]): Hdfs[A] =
    determinefWith(file, directory, Hdfs.fail(s"Not a valid File or Directory. HdfsPath($path)"))

  def determinefWithPure[A](file: HdfsFile => A, directory: HdfsDirectory => A, none: A): Hdfs[A] =
    determinefWith(f => file(f).pure[Hdfs], d => directory(d).pure[Hdfs], none.pure[Hdfs])

  def determinefWith[A](file: HdfsFile => Hdfs[A], directory: HdfsDirectory => Hdfs[A], none: Hdfs[A]): Hdfs[A] =
    determine >>= ({
      case Some(\/-(v)) =>
        directory(v)
      case Some(-\/(v)) =>
        file(v)
      case None =>
        none
    })

  def determineFile: Hdfs[HdfsFile] =
    determinef(_.pure[Hdfs], _ => Hdfs.fail(s"Not a valid file. HdfsDirectory($path)"))

  def determineDirectory: Hdfs[HdfsDirectory] =
    determinef(_ => Hdfs.fail(s"Not a valid directory. HdfsFile($path)"), _.pure[Hdfs])

  def withFileSystem[A](f: FileSystem => A): Hdfs[A] =
    Hdfs.filesystem.flatMap(fs => Hdfs.safe(f(fs)))

  def toInputStream: Hdfs[InputStream] =
    withFileSystem(fs => fs.open(toHPath))

  def toOutputStream: Hdfs[OutputStream] =
    withFileSystem(fs => fs.create(toHPath))

  def toOverwriteOutputStream: Hdfs[OutputStream] =
    withFileSystem(fs => fs.create(toHPath, true))

  def readWith[A](thunk: HdfsFile => Hdfs[A]): Hdfs[A] =
    determinefWith(
        thunk
      , _ => Hdfs.fail(s"Can not read a directory, HdfsDirectory($path).")
      , Hdfs.fail(s"Can not read nothing, HdfsPath($path)."))

  def readWithOption[A](thunk: HdfsFile => Hdfs[Option[A]]): Hdfs[Option[A]] =
    determinefWith(
        thunk
      , _ => Hdfs.fail(s"Can not read a directory, HdfsDirectory($path).")
      , none.pure[Hdfs])

  def read: Hdfs[Option[String]] =
    readWithOption(_.read)

  def readOrFail: Hdfs[String] =
    readWith(_.readOrFail)

  def readWithEncoding(encoding: Codec): Hdfs[Option[String]] =
    readWithOption(_.readWithEncoding(encoding))

  def readLines: Hdfs[Option[List[String]]] =
    readWithOption(_.readLines)

  def readLinesWithEncoding(encoding: Codec): Hdfs[Option[List[String]]] =
    readWithOption(_.readLinesWithEncoding(encoding))

  def doPerLine[A](f: String => Hdfs[Unit]): Hdfs[Unit] =
    readWith(_.doPerLine(f))

  def readPerLine[A](empty: => A)(f: (String, A) => A): Hdfs[A] =
    readWith(_.readPerLine(empty)(f))

  def readPerLineWithEncoding[A](codec: Codec, empty: => A)(f: (String, A) => A): Hdfs[A] =
    readWith(_.readPerLineWithEncoding(codec, empty)(f))

  def readBytes: Hdfs[Option[Array[Byte]]] =
    readWithOption(_.readBytes)

  def readUnsafe[A](f: InputStream => Hdfs[Unit]): Hdfs[Unit] =
    readWith(_.readUnsafe(f))

  def checksum(algorithm: ChecksumAlgorithm): Hdfs[Option[Checksum]] =
    readWithOption(_.checksum(algorithm))

  def lineCount: Hdfs[Option[Int]] =
    readWithOption(_.lineCount)

  def writeExists[A](thunk: => Hdfs[A]): Hdfs[A] =
    doesNotExist(s"A file or directory already exists in the specified location, HdfsPath($path).", thunk)

  def rename(target: HdfsPath): Hdfs[Boolean] =
    withFileSystem(fs => try {
      fs.rename(toHPath, target.toHPath)
    } catch {
      case ioe: IOException =>
        if(ioe.getMessage.startsWith("Target") && ioe.getMessage.endsWith("is a directory")) false
        else throw ioe
    })

  def resolvePath: Hdfs[HdfsPath] =
    withFileSystem(fs => HdfsPath.fromPath(fs.resolvePath(toHPath)))

  def mkdirs: Hdfs[Option[HdfsDirectory]] =
    withFileSystem(fs => if (fs.mkdirs(toHPath)) HdfsDirectory.unsafe(path.path).some else none)

  def mkdirsOrFail: Hdfs[HdfsDirectory] =
    mkdirs.flatMap(o => Hdfs.fromOption(o, s"A file already existed at the specified path, $path"))

  /**
    *  Create a new dir, and if it fails, retry with a new name. This should be atomic
    *
    *  Steps:
    *  1. Create a tmp base dir under /tmp/UUID.randomUUID
    *  2. Create the parent destination dir if it doesn't exist
    *  3. In a loop:
    *    1. Create a new dir under the tmp dir with the name of the destination dir
    *    2. Try moving the new dir to the parent destination dir (using FileSystem.rename)
    *    3. If the move fails, get the next name and try again
    *
    *
    * Treats the internal 'path' as the base directory.
    */
  def mkdirsWithRetry(first: String, nextName: String => Option[String]): Hdfs[Option[HdfsDirectory]] = {
    for {
      _  <- Hdfs.when(first == "")(Hdfs.fail("go away idiot"))
      fs <- Hdfs.filesystem
      t   = HdfsPath.fromString("/tmp") /- java.util.UUID.randomUUID.toString
      q  <- t.mkdirs
      // todo should we resolve here before we mkdirs?
      z  <- mkdirs
      r  <- mkdirsWithRetryX(t, first, nextName, 0)
      _  <- t.delete
    } yield r
  }

  def mkdirsWithRetryX(tmp: HdfsPath, name: String, nextName: String => Option[String], count: Int): Hdfs[Option[HdfsDirectory]] = {
    val source = tmp /- name
    for {
      _ <- Hdfs.when(count > 100)(Hdfs.fail("Number of mkdirs retries exceeded"))
      e <- source.mkdirs
      m <- source.rename(this)
      z <- /-(name).exists
      // add destination exists check
      r <- if (e.isEmpty || m == false) nextName(name).traverse(s => mkdirsWithRetryX(tmp, s, nextName, count + 1)).map(_.flatten)
           else HdfsDirectory.fromHdfsPath(/-(name)).some.pure[Hdfs]
    } yield r
  }

  def writeWith[A](f: OutputStream => Hdfs[A]): Hdfs[A] =
    Hdfs.using(toOutputStream)(o => f(o))

  def touch: Hdfs[Unit] = for {
    e <- exists
    s <- Hdfs.filesystem
    _ <- if (e) Hdfs.safe(s.setTimes(toHPath, System.currentTimeMillis, -1))
         else   write("").void
  } yield ()

  def touchDetermine: Hdfs[HdfsFile\/HdfsDirectory] =
    touch >>
      determinefWith(_.left[HdfsDirectory].pure[Hdfs], _.right[HdfsFile].pure[Hdfs], Hdfs.fail("Invariant"))

  def write(content: String): Hdfs[HdfsFile] =
    writeWithEncoding(content, Codec.UTF8)

  def writeWithEncoding(content: String, encoding: Codec): Hdfs[HdfsFile] = writeExists(for {
    _ <- dirname.mkdirs
    _ <- Hdfs.using(toOutputStream){ out => Hdfs.fromRIO(Streams.writeWithEncoding(out, content, encoding)) }
  } yield HdfsFile.unsafe(path.path))

  def writeLines(content: List[String]): Hdfs[HdfsFile] =
    writeLinesWithEncoding(content, Codec.UTF8)

  def writeLinesWithEncoding(content: List[String], encoding: Codec): Hdfs[HdfsFile] = writeExists(for {
    _ <- dirname.mkdirs
    r <- writeWithEncoding(Lists.prepareForFile(content), encoding)
  } yield r)

  def writeBytes(content: Array[Byte]): Hdfs[HdfsFile] = writeExists(for {
    _ <- dirname.mkdirs
    _ <- Hdfs.using(toOutputStream)(o => Hdfs.fromRIO(Streams.writeBytes(o, content)))
  } yield HdfsFile.unsafe(path.path))

  def writeStream(content: InputStream): Hdfs[HdfsFile] = writeExists(for {
    _ <- dirname.mkdirs
    _ <- Hdfs.using(toOutputStream)(o => Hdfs.fromRIO(Streams.pipe(content, o)))
  } yield HdfsFile.unsafe(path.path))

  def writeWithMode(content: String, mode: HdfsWriteMode): Hdfs[HdfsFile] =
    mode.fold(overwrite(content), write(content))

  def writeWithEncodingMode(content: String, encoding: Codec, mode: HdfsWriteMode): Hdfs[HdfsFile] =
    mode.fold(
        overwriteWithEncoding(content, encoding)
      , writeWithEncoding(content, encoding))

  def writeLinesWithMode(content: List[String], mode: HdfsWriteMode): Hdfs[HdfsFile] =
    mode.fold(overwriteLines(content), writeLines(content))

  def writeLinesWithEncodingMode(content: List[String], encoding: Codec, mode: HdfsWriteMode): Hdfs[HdfsFile] =
    mode.fold(
        overwriteLinesWithEncoding(content, encoding)
      , writeLinesWithEncoding(content, encoding))

  def writeBytesWithMode(content: Array[Byte], mode: HdfsWriteMode): Hdfs[HdfsFile] =
    mode.fold(overwriteBytes(content), writeBytes(content))

  def overwriteStream(content: InputStream): Hdfs[HdfsFile] =
    Hdfs.using(toOverwriteOutputStream)(o =>
      Hdfs.fromRIO(Streams.pipe(content, o))).as(HdfsFile.unsafe(path.path))

  def overwrite(content: String): Hdfs[HdfsFile] =
    overwriteWithEncoding(content, Codec.UTF8)

  def overwriteWithEncoding(content: String, encoding: Codec): Hdfs[HdfsFile] =
    determinefWith(
        _.overwriteWithEncoding(content, encoding).as(HdfsFile.unsafe(path.path))
      , _ => Hdfs.fail(s"Can not overwrite a directory, HdfsDirectory($path)")
      , writeWithEncoding(content, encoding))

  def overwriteLines(content: List[String]): Hdfs[HdfsFile] =
    overwriteLinesWithEncoding(content, Codec.UTF8)

  def overwriteLinesWithEncoding(content: List[String], encoding: Codec): Hdfs[HdfsFile] =
    overwriteWithEncoding(Lists.prepareForFile(content), encoding)

  def overwriteBytes(content: Array[Byte]): Hdfs[HdfsFile] =
    Hdfs.using(toOverwriteOutputStream)(o =>
      Hdfs.fromRIO(Streams.writeBytes(o, content))).as(HdfsFile.unsafe(path.path))

  def move(destination: HdfsPath): Hdfs[HdfsPath] =
    determinef(file =>
      destination.determinefWith(
          _ => Hdfs.fail[Unit](s"File exists in target location $destination. Can not move $path file.")
        , d => file.moveTo(d).void
        , file.move(destination).void)
      , dir =>
      destination.determinefWith(
          _ => Hdfs.fail[Unit](s"File exists in the target location $destination. Can not move $path directory.")
        , d => dir.moveTo(d).void
        , dir.move(destination).void
      )).as(destination)

  def copy(destination: HdfsPath): Hdfs[HdfsPath] =
    determinef(file =>
      destination.determinefWith(
          _ => Hdfs.fail[Unit](s"File exists in target location $destination. Can not move $path file.")
        , d => file.copyTo(d).void
        , file.copy(destination).void)
      , dir =>
        Hdfs.fail[Unit](s"Copying from a HdfsDirectory(path) is current an unsupported operation")).as(destination)

  def size: Hdfs[Option[BytesQuantity]] =
    determinefWith(_.size, _.size, none.pure[Hdfs])

  def sizeOrFail: Hdfs[BytesQuantity] =
    size.flatMap(Hdfs.fromOption(_, s"Can not calculate the size of a path that does not exist, HdfsPath($path)."))

  def numberOfFiles: Hdfs[Option[Int]] =
    determinefWith(_ => 1.some.pure[Hdfs], _.numberOfFiles, none.pure[Hdfs])

  def numberOfFilesOrFail: Hdfs[Int] =
    numberOfFiles.flatMap(Hdfs.fromOption(_,
      s"Can not calculate the number of files within a path that does not exist, HdfsPath($path)."))

  def globFiles(glob: String): Hdfs[List[HdfsFile]] =
    determinef(v => List(v).pure[Hdfs], _.globFiles(glob))

  def globDirectories(glob: String): Hdfs[List[HdfsDirectory]] =
    determinef(_ => nil.pure[Hdfs], _.globDirectories(glob))

  def globPaths(glob: String): Hdfs[List[HdfsPath]] =
    determinef(_ => List(this).pure[Hdfs], _.globPaths(glob))

  def listFiles: Hdfs[List[HdfsFile]] =
    globFiles("*")

  def listDirectories: Hdfs[List[HdfsDirectory]] =
    globDirectories("*")

  def listPaths: Hdfs[List[HdfsPath]] =
    globPaths("*")

  def listFilesRelativeTo: Hdfs[List[(HdfsFile, HdfsPath)]] =
    determinef(_ => nil.pure[Hdfs], _.listFilesRelativeTo)

  def listFilesRecursivelyRelativeTo: Hdfs[List[(HdfsFile, HdfsPath)]] =
    determinef(_ => nil.pure[Hdfs], _.listFilesRelativeTo)

  def listFilesRecursively: Hdfs[List[HdfsFile]] =
    determinef(f => List(f).pure[Hdfs], _.listFilesRecursively)

  def listDirectoriesRecursively: Hdfs[List[HdfsDirectory]] =
    determinef(f => nil.pure[Hdfs], _.listDirectoriesRecursively)

  def listPathsRecursively: Hdfs[List[HdfsPath]] =
    determinef(f => List(this).pure[Hdfs], _.listPathsRecursively)
}

object HdfsPath {
  def fromPath(p: HPath): HdfsPath =
    fromString(p.toUri.getPath)

  def fromString(s: String): HdfsPath =
    HdfsPath(Path(s))

  def fromList(dir: Path, components: List[Component]): HdfsPath =
    HdfsPath(Path.fromList(dir, components))

  def fromURI(s: URI): Option[HdfsPath] =
    s.getScheme match {
      case "hdfs" =>
        fromString(s.getPath).some
      case null =>
        fromString(s.getPath).some
      case _ =>
        none
    }

  def filterHidden(l: List[HdfsPath]): List[HdfsPath] =
    l.filter(f => !List(".", "_").exists(c => f.path.basename.exists(_.name.startsWith(c))))

  implicit def HdfsPathOrder: Order[HdfsPath] =
    Order.order((x, y) => x.path.?|?(y.path))

  implicit def HdfsPathOrdering: scala.Ordering[HdfsPath] =
    HdfsPathOrder.toScalaOrdering
}
