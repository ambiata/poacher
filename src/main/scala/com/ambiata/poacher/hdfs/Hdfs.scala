package com.ambiata.poacher.hdfs

import java.util.UUID

import scalaz._, Scalaz._, \&/._, effect._, Effect._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, FileUtil, Path}
import java.io._

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{BytesQuantity, Streams, MemoryConversions}
import MemoryConversions._

case class Hdfs[A](run: Configuration => RIO[A]) {
  def map[B](f: A => B): Hdfs[B] =
    Hdfs(run(_).map(f))

  def flatMap[B](f: A => Hdfs[B]): Hdfs[B] =
    Hdfs(c => run(c).flatMap(a => f(a).run(c)))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): Hdfs[A] =
    Hdfs(run(_).mapError(f))

  def mapErrorString(f: String => String): Hdfs[A] =
    Hdfs(run(_).mapError(_.leftMap(f)))

  def |||(other: Hdfs[A]): Hdfs[A] =
    Hdfs(c => run(c) ||| other.run(c))

  def filterHidden(implicit ev: A <:< List[Path]): Hdfs[List[Path]] =
    map(_.filter(p => !p.getName.startsWith("_") && !p.getName.startsWith(".")))

  def unless(condition: Boolean): Hdfs[Unit] =
    Hdfs.unless(condition)(this)
}

object Hdfs {
  def addFinalizer(finalizer: Hdfs[Unit]): Hdfs[Unit] =
    Hdfs(c => RIO.addFinalizer(Finalizer(finalizer.run(c))))

  def unsafeFlushFinalizers: Hdfs[Unit] =
    Hdfs.fromRIO(RIO.unsafeFlushFinalizers)

  def ok[A](a: A): Hdfs[A] =
    Hdfs(_ => RIO.ok(a))

  def safe[A](a: => A): Hdfs[A] =
    Hdfs(_ => RIO.safe(a))

  def io[A](a: => A): Hdfs[A] =
    Hdfs(_ => RIO.io(a))

  def result[A](a: => Result[A]): Hdfs[A] =
    Hdfs(_ => RIO.result(a))

  def fail[A](e: String): Hdfs[A] =
    Hdfs(_ => RIO.fail(e))

  def fromDisjunction[A](d: String \/ A): Hdfs[A] = d match {
    case -\/(e) => fail(e)
    case \/-(a) => ok(a)
  }

  def fromValidation[A](v: Validation[String, A]): Hdfs[A] =
    fromDisjunction(v.disjunction)

  def fromOption[A](o: Option[A], err: String): Hdfs[A] =
    o.map(ok).getOrElse(fail(err))

  def fromIO[A](io: IO[A]): Hdfs[A] =
    Hdfs(_ => RIO.fromIO(io))

  def fromRIO[A](res: RIO[A]): Hdfs[A] =
    Hdfs(_ => res)

  def filesystem: Hdfs[FileSystem] =
    Hdfs(c => RIO.safe(FileSystem.get(c)))

  def filecontext: Hdfs[FileContext] =
    Hdfs(c => RIO.safe(FileContext.getFileContext(c)))

  def configuration: Hdfs[Configuration] =
    Hdfs(_.pure[RIO])

  def exists(p: Path): Hdfs[Boolean] =
    filesystem.map(fs => fs.exists(p))

  /** @return the size of a directory not including all directories */
  def size(p: Path): Hdfs[BytesQuantity] = for {
    fs    <- filesystem
    files <- if (fs.isFile(p)) Hdfs.value(List(p)) else globFiles(p)
  } yield files.map(f => fs.getFileStatus(f).getLen.bytes).sum

  /** @return the size of a directory, recursively including all directories */
  def totalSize(path: Path): Hdfs[BytesQuantity] =
    for {
      all <- Hdfs.globPathsRecursively(path)
      sizes <- all.traverse(Hdfs.size)
    } yield sizes.sum

  /** @return the number of files in a directory */
  def numberOfFiles(path: Path, glob: String = "*"): Hdfs[Int] =
    Hdfs.globPaths(path, glob).map(_.size)

  /** @return the number of files in a directory, including the files in subdirectories */
  def numberOfFilesRecursively(path: Path, glob: String = "*"): Hdfs[Int] =
    Hdfs.globPathsRecursively(path, glob).map(_.size)

  def isDirectory(p: Path): Hdfs[Boolean] =
    filesystem.map(fs => fs.isDirectory(p))

  def mustExist(p: Path): Hdfs[Unit] =
    mustExistWithMessage(p, s"$p doesn't exist!")

  def mustExistWithMessage(p: Path, error: String): Hdfs[Unit] =
    exists(p).flatMap(e => if(e) Hdfs.ok(()) else Hdfs.fail(error))

  def mustNotExist(p: Path): Hdfs[Unit] =
    mustNotExistWithMessage(p, s"$p should not exist!")

  def mustNotExistWithMessage(p: Path, error: String): Hdfs[Unit] =
    exists(p).flatMap(e => if(e) Hdfs.fail(error) else Hdfs.ok(()))

  def globDirs(p: Path, glob: String = "*"): Hdfs[List[Path]] =
    filesystem.map(fs =>
      if (fs.isFile(p)) List() else fs.globStatus(new Path(p, glob)).toList.filter(_.isDirectory).map(_.getPath)
    )

  def globPaths(p: Path, glob: String = "*"): Hdfs[List[Path]] =
    filesystem.map(fs =>
      if(fs.isFile(p)) List(p) else fs.globStatus(new Path(p, glob)).toList.map(_.getPath))

  def globPathsRecursively(p: Path, glob: String = "*"): Hdfs[List[Path]] = {
    def getPaths(path: Path): FileSystem => List[Path] = { fs: FileSystem =>
      if (fs.isFile(path)) List(path)
      else {
        val paths = fs.globStatus(new Path(path, glob)).toList.map(_.getPath)
        (paths ++ paths.flatMap(p1 => fs.listStatus(p1).flatMap(s => getPaths(s.getPath)(fs)))).distinct
      }
    }
    filesystem.map(getPaths(p))
  }

  def globFiles(p: Path, glob: String = "*"): Hdfs[List[Path]] = for {
    fs    <- filesystem
    files <- globPaths(p, glob)
  } yield files.filter(fs.isFile)

  def globFilesRecursively(p: Path, glob: String = "*"): Hdfs[List[Path]] = for {
    fs    <- filesystem
    files <- globPathsRecursively(p, glob)
  } yield files.filter(fs.isFile)

  /**
   * strip out the non-glob path and the glob path of a path
   * if the path has no globbing characters then the glob is "*"
   */
  def pathAndGlob(path: Path): (Path, String) = {
    val parts = path.toUri.toString.split("/")
    val (basePath, glob) = parts.partition(p  => !Seq("*", "{").exists(p.contains))
    (new Path(basePath.mkString("/")), if (glob.isEmpty) "*" else glob.mkString("/"))
  }

  def readWith[A](p: Path, f: InputStream => RIO[A], glob: String = "*"): Hdfs[A] = for {
    _     <- mustExist(p)
    paths <- globFiles(p, glob)
    a     <- filesystem.flatMap(fs => {
      if(!paths.isEmpty) {
        val is = paths.map(fs.open).reduce[InputStream]((a, b) => new SequenceInputStream(a, b))
        Hdfs.fromRIO(RIO.using(RIO.safe[InputStream](is)) { in =>
          f(is)
        })
      } else {
        Hdfs.fail[A](s"No files found for path $p!")
      }
    })
  } yield a

  def readContentAsString(p: Path): Hdfs[String] =
    readWith(p, is =>  Streams.read(is))

  def readLines(p: Path): Hdfs[Iterator[String]] =
    readContentAsString(p).map(_.lines)

  def globLines(p: Path, glob: String = "*"): Hdfs[Iterator[String]] =
    Hdfs.globFiles(p, glob).flatMap(_.map(Hdfs.readLines).sequenceU.map(_.toIterator.flatten))

  def write(p: Path, content: String): Hdfs[Unit] = for {
    _ <- Hdfs.safe(mkdir(p.getParent)).void
    f <- filesystem
    _ <- Hdfs.fromRIO(RIO.using(RIO.safe[OutputStream](f.create(p))) { out =>
      Streams.write(out, content)
    })
  } yield ()

  def writeWith[A](p: Path, f: OutputStream => RIO[A]): Hdfs[A] = for {
    _ <- mustExist(p) ||| mkdir(p.getParent).void
    a <- filesystem.flatMap(fs =>
      Hdfs.fromRIO(RIO.using(RIO.safe[OutputStream](fs.create(p))) { out =>
        f(out)
      }))
  } yield a

  def cp(src: Path, dest: Path, overwrite: Boolean): Hdfs[Unit] = for {
    fs   <- filesystem
    conf <- configuration
    res  <- Hdfs.value(FileUtil.copy(fs, src, fs, dest, false, overwrite, conf))
    _    <- if(!res && overwrite) fail(s"Could not copy $src to $dest") else ok(())
  } yield ()

  def mkdir(p: Path): Hdfs[Boolean] =
    filesystem.map(fs => fs.mkdirs(p))

  /**
   * Create a new dir, and if it fails, retry with a new name. This should be atomic
   *
   * Steps:
   * 1. Create a tmp base dir under /tmp/UUID.randomUUID
   * 2. Create the parent destination dir if it doesn't exist
   * 3. In a loop:
   *   1. Create a new dir under the tmp dir with the name of the destination dir
   *   2. Try moving the new dir to the parent destination dir (using FileSystem.rename)
   *   3. If the move fails, get the next name and try again
   */
  def mkdirWithRetry(p: Path, nextName: String => Option[String]): Hdfs[Option[Path]] =
    filesystem.map(fs => {
      val tmp = new Path("/tmp", UUID.randomUUID.toString)
      fs.mkdirs(tmp)
      val parent = p.getParent
      fs.mkdirs(parent)
      val names = Stream.iterate[Option[String]](Some(p.getName))(_.flatMap(nextName))
      names.dropWhile({
        case None    => false
        case Some(n) =>
          val tp = new Path(tmp, n)
          try { !fs.mkdirs(tp) || !fs.rename(tp, parent) }
          // hack to catch local mode inconsistency
          catch { case ioe: IOException => if(ioe.getMessage.startsWith("Target") && ioe.getMessage.endsWith("is a directory")) true else throw ioe }
      }).headOption.flatten.map(n => new Path(parent, n))
    })

  def mv(src: Path, dest: Path): Hdfs[Path] = for {
    fs <- filesystem
    newDest <- Hdfs.safe {
      // Evil. Unfortunate moving dirs to dirs in HDFS is (intentionally) broken in local mode
      // https://issues.apache.org/jira/browse/HADOOP-9507
      if (fs.getScheme == "file" && fs.isDirectory(src) && fs.isDirectory(dest))
        new Path(dest, src.getName)
      else dest
    }
    r <- if(fs.rename(src, newDest)) Hdfs.value(dest) else Hdfs.fail(s"Could not move ${src} to ${dest}")
  } yield r

  def delete(p: Path): Hdfs[Unit] =
    filesystem.map(_.delete(p, false)).void

  def deleteAll(p: Path): Hdfs[Unit] =
    filesystem.map(_.delete(p, true)).void

  def log(message: String) =
    fromIO(IO(println(message)))

  def when[A](condition: Boolean)(action: Hdfs[A]): Hdfs[Unit] =
    if (condition) action.map(_ => ()) else Hdfs.ok(())

  def unless[A](condition: Boolean)(action: Hdfs[A]): Hdfs[Unit] =
    when(!condition)(action)

  def using[A: Resource, B <: A, C](a: Hdfs[B])(run: B => Hdfs[C]): Hdfs[C] =
    Hdfs(c => RIO.using[A, B, C](a.run(c))(b => run(b).run(c)))

  /**
   * @return a list of all subdirectories names (from path) with their total size
   */
  def childrenSizes(path: Path, glob: String = "*"): Hdfs[List[(Path, BytesQuantity)]] =
    for {
      children <- Hdfs.globPaths(path, glob)
      sizes    <- children.traverse(c => totalSize(c).map((c, _)))
    } yield sizes

  implicit def HdfsMonad: Monad[Hdfs] = new Monad[Hdfs] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: Hdfs[A])(f: A => Hdfs[B]) = m.flatMap(f)
  }

  def putStrLn(msg: String): Hdfs[Unit] =
    Hdfs(_ => RIO.putStrLn(msg))

  def unit: Hdfs[Unit] =
    Hdfs(_ => RIO.unit)
}
