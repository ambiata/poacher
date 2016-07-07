package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.data._
import com.ambiata.mundane.io._
import com.ambiata.mundane.path._

import java.io._
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, FileUtil}

import scala.io.Codec
import scalaz._, Scalaz._, effect.Effect._
import MemoryConversions._

class HdfsFile private (val path: Path) extends AnyVal {
  override def toString: String =
    s"HdfsFile($path)"

  def toHdfsPath: HdfsPath =
    HdfsPath(path)

  def toHPath: HPath =
    new HPath(path.path)

  def withFileSystem[A](f: FileSystem => A): Hdfs[A] =
    Hdfs.filesystem.flatMap(fs => Hdfs.safe(f(fs)))

  def toInputStream: Hdfs[InputStream] =
    withFileSystem(fs => fs.open(toHPath))

  def toInputStreamReader: Hdfs[java.io.Reader] =
    withFileSystem(fs => new BufferedReader(new InputStreamReader(fs.open(toHPath))))

  def toOutputStream: Hdfs[OutputStream] =
    withFileSystem(fs => fs.create(toHPath, true))

  def exists: Hdfs[Boolean] = for {
    f <- Hdfs.filesystem
    e <- Hdfs.safe(f.isFile(toHPath))
    d <- Hdfs.safe(f.isDirectory(toHPath))
    r <- if (e)      Hdfs.ok(true)
         else if (d) Hdfs.fail(s"An internal invariant has been violated, the $path points to a directory.")
         else        Hdfs.ok(false)
  } yield r

  def onExists[A](success: => Hdfs[A], missing: => Hdfs[A]): Hdfs[A] =
    exists >>= (e =>
      if (e) success
      else   missing
    )

  def optionExists[A](thunk: => Hdfs[A]): Hdfs[Option[A]] =
    onExists(thunk.map(_.some), none.pure[Hdfs])

  def whenExists(thunk: => Hdfs[Unit]): Hdfs[Unit] =
    onExists(thunk, Hdfs.unit)

  def doesExist[A](error: String, thunk: => Hdfs[A]): Hdfs[A] =
    onExists(thunk, Hdfs.fail(error))

  def doesNotExist[A](error: String, thunk: => Hdfs[A]): Hdfs[A] =
    onExists(Hdfs.fail(error), thunk)

  def delete: Hdfs[Unit] =
    Hdfs.filesystem.map(_.delete(toHPath, false)).void

  def lastModified: Hdfs[Long] =
    withFileSystem(fs => fs.getFileStatus(toHPath).getModificationTime)

  def readWith[A](f: InputStream => Hdfs[A]): Hdfs[A] =
    Hdfs.using(toInputStream)(i => f(i))

  def read: Hdfs[Option[String]] =
    readWithEncoding(Codec.UTF8)

  def readOrFail: Hdfs[String] =
    read.flatMap(Hdfs.fromOption(_, s"Failed to read file - HdfsFile($path) does not exist"))

  def readWithEncoding(encoding: Codec): Hdfs[Option[String]] =
    optionExists(readWith(in => Hdfs.fromRIO(Streams.readWithEncoding(in, encoding))))

  def readLines: Hdfs[Option[List[String]]] =
    readLinesWithEncoding(Codec.UTF8)

  def readLinesWithEncoding(encoding: Codec): Hdfs[Option[List[String]]] =
    optionExists(readPerLineWithEncoding(encoding,
      scala.collection.mutable.ListBuffer[String]())((s, b) => { b += s; b }).map(_.toList))

  def doPerLine[A](f: String => Hdfs[Unit]): Hdfs[Unit] = for {
    c <- Hdfs.configuration
    _ <- readWith(in =>
      Hdfs.io {
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))
        var line: String = null
        var result: Hdfs[Unit] = null
        while ({ line = reader.readLine; line != null && result == null })
          f(line).run(c).unsafePerformIO match {
            case Ok(_) =>
              ()
            case e @ Error(_) =>
              result = Hdfs.result[Unit](e)
          }
        }
    )
  } yield ()

  def readPerLine[A](empty: => A)(f: (String, A) => A): Hdfs[A] =
    readPerLineWithEncoding(Codec.UTF8, empty)(f)

  def readPerLineWithEncoding[A](codec: Codec, empty: => A)(f: (String, A) => A): Hdfs[A] =
    Hdfs.io(empty).flatMap { s =>
      var state = s
      readUnsafe { in => Hdfs.io {
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, codec.name))
        var line: String = null
        while ({ line = reader.readLine; line != null })
          state = f(line, state)
      }}.as(state)
    }

  def readBytes: Hdfs[Option[Array[Byte]]] =
    optionExists(readWith(o => Hdfs.fromRIO(Streams.readBytes(o))))

  def readUnsafe(f: InputStream => Hdfs[Unit]): Hdfs[Unit] =
    readWith(f).void

  def size: Hdfs[Option[BytesQuantity]] =
    optionExists(withFileSystem(_.getFileStatus(toHPath).getLen.bytes))

  def sizeOrFail: Hdfs[BytesQuantity] =
    size.flatMap(Hdfs.fromOption(_, s"Can not calculate the size of a file that does not exist, HdfsFile($path)."))

  def checksum(algorithm: ChecksumAlgorithm): Hdfs[Option[Checksum]] =
    optionExists(Hdfs.using(toInputStream)(o => Hdfs.fromRIO(Checksum.stream(o, algorithm))))

  def lineCount: Hdfs[Option[Int]] =
    optionExists(for {
      i <- toInputStreamReader
      r <- Hdfs.io({
        val reader = new LineNumberReader(i)
        try {
          while (reader.readLine != null) {}
            reader.getLineNumber
        } finally reader.close
      })
    } yield r)

  def overwriteWith[A](f: OutputStream => Hdfs[A]): Hdfs[A] =
    Hdfs.using(toOutputStream)(o => f(o))

  def overwriteStream(content: InputStream): Hdfs[Unit] =
    overwriteWith(o => Hdfs.fromRIO(Streams.pipe(content, o)))

  def overwrite(content: String): Hdfs[Unit] =
    overwriteWithEncoding(content, Codec.UTF8)

  def overwriteWithEncoding(content: String, encoding: Codec): Hdfs[Unit] =
    overwriteWith(out => Hdfs.fromRIO(Streams.writeWithEncoding(out, content, encoding)))

  def overwriteLines(content: List[String]): Hdfs[Unit] =
    overwriteLinesWithEncoding(content, Codec.UTF8)

  def overwriteLinesWithEncoding(content: List[String], encoding: Codec): Hdfs[Unit] =
    overwriteWithEncoding(Lists.prepareForFile(content), encoding)

  def overwriteBytes(content: Array[Byte]): Hdfs[Unit] =
    overwriteWith(o => Hdfs.fromRIO(Streams.writeBytes(o, content)))

  def move(destination: HdfsPath): Hdfs[HdfsFile] =
    doesExist(s"Source file does not exist. HdfsFile($path)",
      destination.doesNotExist(s"A file/directory exists in target location $destination. Can not move source file HdfsFile($path)",
        for {
          _ <- destination.dirname.mkdirs // this will only happen as part of the rename below in local mode
          m <- withFileSystem(_.rename(toHPath, destination.toHPath))
          _ <- Hdfs.unless(m)(Hdfs.fail(s"Failed to move HdfsFile($path) to HdfsFile($destination)"))
        } yield HdfsFile.unsafe(destination.path.path)))

  def moveWithMode(destination: HdfsPath, mode: TargetMode): Hdfs[HdfsFile] =
    mode.fold(doesExist(s"Source file does not exist. HdfsFile($path)",
      destination.whenExists(destination.delete) >>
        move(destination)
    ), move(destination))

  def moveTo(destination: HdfsDirectory): Hdfs[HdfsFile] =
    (path.basename match {
      case None =>
        Hdfs.fail(s"Source is a top level directory, can't move. HdfsFile($path)")
      case Some(filename) =>
        move(destination.toHdfsPath | filename)
    })

  def copy(destination: HdfsPath): Hdfs[HdfsFile] =
    doesExist(s"Source file does not exist. HdfsFile($path)",
      destination.doesNotExist(s"A file/directory exists in target location $destination. Can not move source file HdfsFile($path)",
        copyUnsafeWithOverwrite(destination, false)))

  def copyWithMode(destination: HdfsPath, mode: TargetMode): Hdfs[HdfsFile] =
    mode.fold(doesExist(s"Source file does not exist. HdfsFile($path)",
      copyUnsafeWithOverwrite(destination, true)), copy(destination))

  def copyUnsafeWithOverwrite(destination: HdfsPath, overwrite: Boolean): Hdfs[HdfsFile] = for {
    s <- Hdfs.filesystem
    c <- Hdfs.configuration
    m <- Hdfs.safe(FileUtil.copy(s, toHPath, s, destination.toHPath, false, overwrite, c))
    _ <- Hdfs.unless(m)(Hdfs.fail(s"Failed to copy HdfsFile($path) to HdfsFile($destination)"))
  } yield HdfsFile.unsafe(destination.path.path)

  def copyTo(destination: HdfsDirectory): Hdfs[HdfsFile] =
    path.basename match {
      case None =>
        Hdfs.fail(s"Source is a top level directory, can't copy. Source($path)")
      case Some(filename) =>
        copy(destination.toHdfsPath | filename)
    }
}

object HdfsFile {
  def fromString(s: String): Hdfs[Option[HdfsFile]] =
    HdfsPath.fromString(s).determinefWithPure(_.some, _ => none, none)

  def fromPath(p: HPath): Hdfs[Option[HdfsFile]] =
    HdfsPath.fromPath(p).determinefWithPure(_.some, _ => none, none)

  def fromList(dir: Path, parts: List[Component]): Hdfs[Option[HdfsFile]] =
    HdfsPath.fromList(dir, parts).determinefWithPure(_.some, _ => none, none)

  def fromURI(s: URI): Hdfs[Option[HdfsFile]] =
    HdfsPath.fromURI(s).traverse(_.determinefWithPure(_.some, _ => none, none)).map(_.flatten)

  private[hdfs] def unsafe(s: String): HdfsFile =
    new HdfsFile(Path(s))

  implicit def HdfsFileOrder: Order[HdfsFile] =
    Order.order((x, y) => x.path.?|?(y.path))

  implicit def HdfsFileOrdering: scala.Ordering[HdfsFile] =
    HdfsFileOrder.toScalaOrdering
}
