package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.path._
import com.ambiata.mundane.io._

import java.io._
import java.net.URI

import org.apache.hadoop.fs.FileSystem

import scalaz._, Scalaz._, effect.Effect._

/**
 * 'HdfsDirectory' represents a directory that exists on a hdfs file system
 */
class HdfsDirectory private (val path: Path) extends AnyVal {
  override def toString: String =
    s"HdfsDirectory($path)"

  def toHdfsPath: HdfsPath =
    HdfsPath(path)

  def toHPath: HPath =
    new HPath(path.path)

  def exists: Hdfs[Boolean] = for {
    f <- Hdfs.filesystem
    d <- Hdfs.safe(f.isDirectory(toHPath))
    e <- Hdfs.safe(f.isFile(toHPath))
    r <- if (d)      Hdfs.ok(true)
         else if (e) Hdfs.fail(s"An internal invariant has been violated, the $path points to a file.")
         else        Hdfs.ok(false)
  } yield r

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

  def withFileSystem[A](f: FileSystem => A): Hdfs[A] =
    Hdfs.filesystem.flatMap(fs => Hdfs.safe(f(fs)))

  def mkdirs: Hdfs[Boolean] =
    withFileSystem(_.mkdirs(toHPath))

  def delete: Hdfs[Unit] =
    withFileSystem(_.delete(toHPath, true)).void

  def parent: Option[HdfsDirectory] =
    path.parent.map(new HdfsDirectory(_))

  // List all things

  def move(destination: HdfsPath): Hdfs[HdfsDirectory] =
    path.basename match {
      case None =>
        Hdfs.fail("Source is a top level directory, can't move")
      case Some(_) =>
        doesExist(s"Source directory does not exists. HdfsDirectory($path)",
          destination.doesNotExist(s"A file/directory exists in the target location $destination. Can not move source directory HdfsDirectory($path).", for {
            s <- Hdfs.filesystem
            d <- Hdfs.safe({
              // Evil. Unfortunate moving dirs to dirs in HDFS is (intentionally) broken in local mode
              // https://issues.apache.org/jira/browse/HADOOP-9507
              if (s.getScheme == "file" && s.isDirectory(toHPath) && s.isDirectory(destination.toHPath))
                new HPath(destination.toHPath, toHPath.getName)
              else destination.toHPath
            })
            m <- Hdfs.safe(s.rename(toHPath, d))
            _ <- Hdfs.unless(m)(Hdfs.fail(s"Could not move HdfsDirectory($path) to HdfsPath($destination)"))
          } yield HdfsDirectory.unsafe(destination.path.path)))
    }

  def moveWithMode(destination: HdfsPath, mode: TargetMode): Hdfs[HdfsDirectory] =
    mode.fold(doesExist(s"Source directory does not exists. HdfsDirectory($path)",
      destination.delete >> move(destination)), move(destination))

  def moveTo(destination: HdfsDirectory): Hdfs[HdfsDirectory] =
    path.basename match {
      case None =>
        Hdfs.fail("Source is a top level directory, can't move")
      case Some(filename) =>
        move(destination.toHdfsPath | filename)
    }

  def size: Hdfs[Option[BytesQuantity]] = for {
    l <- listFilesRecursively
    s <- l.traverse(_.sizeOrFail)
  } yield (!s.isEmpty).option(s.sum)

  def sizeOrFail: Hdfs[BytesQuantity] =
    size.flatMap(Hdfs.fromOption(_, s"Can not calculate the size of a directory that does not exist, HdfsDirectory($path)."))

  /** @return the number of files in a directory, including the files in subdirectories but not the subdirectories */
  def numberOfFiles: Hdfs[Option[Int]] =
    listFilesRecursively.map(x => (!x.isEmpty).option(x.size))

  def listFiles: Hdfs[List[HdfsFile]] =
    globFiles("*")

  def listDirectories: Hdfs[List[HdfsDirectory]] =
    globDirectories("*")

  def listPaths: Hdfs[List[HdfsPath]] =
    globPaths("*")

  def globFiles(glob: String): Hdfs[List[HdfsFile]] = for {
    g <- globPaths(glob)
    r <- g.traverse(_.determine)
  } yield r.map(_.map(_.swap.toOption)).flatten.flatten

  def globDirectories(glob: String): Hdfs[List[HdfsDirectory]] = for {
    g <- globPaths(glob)
    r <- g.traverse(_.determine)
  } yield r.map(_.map(_.toOption)).flatten.flatten

  def globPaths(glob: String): Hdfs[List[HdfsPath]] =
    withFileSystem(fs =>
      if (fs.isFile(toHPath)) List(toHdfsPath)
      else fs.globStatus(new HPath(toHPath, glob)).toList.map(f => HdfsPath.fromPath(f.getPath)))

  /**
   *  strip out the non-glob path and the glob path of a path
   *  if the path has no globbing characters then the glob is "*"
   */
  def pathAndGlob: (HPath, String) = {
    val parts = toHPath.toUri.toString.split("/")
    val (basePath, glob) = parts.partition(p => !Seq("*", "{").exists(p.contains))
    (new HPath(basePath.mkString("/")), if (glob.isEmpty) "*" else glob.mkString("/"))
  }

  def listFilesRelativeTo: Hdfs[List[(HdfsFile, HdfsPath)]] =
    listFiles.flatMap(_.traverseU(f =>
      Hdfs.fromOption[HdfsPath](f.toHdfsPath.rebaseTo(toHdfsPath),
        "Invariant failure, this is likely a bug: https://github.com/ambiata/poacher/issues").map(f -> _)))

  def listFilesRecursivelyRelativeTo: Hdfs[List[(HdfsFile, HdfsPath)]] =
    listFilesRecursively.flatMap(_.traverseU(f =>
      Hdfs.fromOption[HdfsPath](f.toHdfsPath.rebaseTo(toHdfsPath),
        "Invariant failure, this is likely a bug: https://github.com/ambiata/poacher/issues").map(f -> _)))

  def listFilesRecursively: Hdfs[List[HdfsFile]] =
    withFileSystem(fs => {
      def loop(p: HdfsPath): List[HdfsFile] = {
        fs.globStatus(new HPath(p.toHPath, "*")).toList.flatMap(f => {
          val pp = p | Component.unsafe(f.getPath.getName)
          if (f.isFile) List(HdfsFile.unsafe(pp.path.path))
          else          loop(pp)
        })
      }
      loop(toHdfsPath)
    })

  def listDirectoriesRecursively: Hdfs[List[HdfsDirectory]] =
    withFileSystem(fs => {
      def loop(p: HdfsPath): List[HdfsDirectory] = {
        fs.globStatus(new HPath(p.toHPath, "*")).toList.flatMap(f => {
          val pp = p | Component.unsafe(f.getPath.getName)
          if (f.isDirectory) List(HdfsDirectory.unsafe(pp.path.path)) ++ loop(pp)
          else               List()
        })
      }
      loop(toHdfsPath)
    })

  def listPathsRecursively: Hdfs[List[HdfsPath]] =
    withFileSystem(fs => {
      def loop(p: HdfsPath): List[HdfsPath] = {
        fs.globStatus(new HPath(p.toHPath, "*")).toList.flatMap(f => {
          val pp = p | Component.unsafe(f.getPath.getName)
          if (f.isDirectory) List(pp) ++ loop(pp)
          else               List(pp)
        })
      }
      loop(toHdfsPath)
    })

}

object HdfsDirectory {
  def fromString(s: String): Hdfs[Option[HdfsDirectory]] =
    HdfsPath.fromString(s).determinefWithPure(_ => none, _.some, none)

  def fromPath(p: HPath): Hdfs[Option[HdfsDirectory]] =
    HdfsPath.fromPath(p).determinefWithPure(_ => none, _.some, none)

  def fromList(dir: Path, parts: List[Component]): Hdfs[Option[HdfsDirectory]] =
    HdfsPath.fromList(dir, parts).determinefWithPure(_ => none, _.some, none)

  def fromURI(s: URI): Hdfs[Option[HdfsDirectory]] =
    HdfsPath.fromURI(s).traverse(_.determinefWithPure(_ => none, _.some, none)).map(_.flatten)

  private[hdfs] def unsafe(s: String): HdfsDirectory =
    new HdfsDirectory(Path(s))

  private[hdfs] def fromHdfsPath(p: HdfsPath): HdfsDirectory =
    new HdfsDirectory(p.path)

  implicit def HdfsDirectoryOrder: Order[HdfsDirectory] =
    Order.order((x, y) => x.path.?|?(y.path))

  implicit def HdfsFileOrdering: scala.Ordering[HdfsDirectory] =
    HdfsDirectoryOrder.toScalaOrdering
}
