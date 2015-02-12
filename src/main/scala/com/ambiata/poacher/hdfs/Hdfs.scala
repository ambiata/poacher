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

  def log(message: String): Hdfs[Unit] =
    fromIO(IO(println(message)))

  def when[A](condition: Boolean)(action: Hdfs[A]): Hdfs[Unit] =
    if (condition) action.map(_ => ()) else Hdfs.ok(())

  def unless[A](condition: Boolean)(action: Hdfs[A]): Hdfs[Unit] =
    when(!condition)(action)

  def using[A: Resource, B <: A, C](a: Hdfs[B])(run: B => Hdfs[C]): Hdfs[C] =
    Hdfs(c => RIO.using[A, B, C](a.run(c))(b => run(b).run(c)))

  implicit def HdfsMonad: Monad[Hdfs] = new Monad[Hdfs] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: Hdfs[A])(f: A => Hdfs[B]) = m.flatMap(f)
  }

  def putStrLn(msg: String): Hdfs[Unit] =
    Hdfs(_ => RIO.putStrLn(msg))

  def unit: Hdfs[Unit] =
    Hdfs(_ => RIO.unit)
}
