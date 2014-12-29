package com.ambiata.poacher
package scoobi

import com.ambiata.mundane.control._
import com.nicta.scoobi.core.ScoobiConfiguration
import com.nicta.scoobi.core.ScoobiConfiguration
import scalaz.effect.IO
import scalaz._, Scalaz._
import \&/._
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.fs.{Path, FileSystem}
import hdfs._

case class ScoobiAction[A](run: ScoobiConfiguration => RIO[A]) {
  def map[B](f: A => B): ScoobiAction[B] =
    ScoobiAction(run(_).map(f))

  def flatMap[B](f: A => ScoobiAction[B]): ScoobiAction[B] =
    ScoobiAction(c => run(c).flatMap(a => f(a).run(c)))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): ScoobiAction[A] =
    ScoobiAction(run(_).mapError(f))

  def mapErrorString(f: String => String): ScoobiAction[A] =
    ScoobiAction(run(_).mapError(_.leftMap(f)))

  def |||(other: ScoobiAction[A]): ScoobiAction[A] =
    ScoobiAction(c => run(c) ||| other.run(c))

  def flatten[B](implicit ev: A <:< ScoobiAction[B]): ScoobiAction[B] =
    flatMap(a => ev(a))

  def unless(condition: Boolean): ScoobiAction[Unit] =
    ScoobiAction.unless(condition)(this)

  def log(f: A => String) =
    flatMap((a: A) => ScoobiAction.log(f(a)))
}


object ScoobiAction {

  def value[A](a: A): ScoobiAction[A] =
    ScoobiAction(_ => RIO.ok(a))

  def ok[A](a: A): ScoobiAction[A] =
    value(a)

  def safe[A](a: => A): ScoobiAction[A] =
    ScoobiAction(_ => RIO.safe(a))

  def fail[A](e: String): ScoobiAction[A] =
    ScoobiAction(_ => RIO.fail(e))

  def fromDisjunction[A](d: String \/ A): ScoobiAction[A] = d match {
    case -\/(e) => fail(e)
    case \/-(a) => ok(a)
  }

  def fromValidation[A](v: Validation[String, A]): ScoobiAction[A] =
    fromDisjunction(v.disjunction)

  def fromIO[A](io: IO[A]): ScoobiAction[A] =
    ScoobiAction(_ => RIO.fromIO(io))

  def fromRIO[A](res: RIO[A]): ScoobiAction[A] =
    ScoobiAction(_ => res)

  def fromHdfs[A](action: Hdfs[A]): ScoobiAction[A] = for {
    sc <- ScoobiAction.scoobiConfiguration
    a  <- ScoobiAction.fromRIO(action.run(sc.configuration))
  } yield a

  def filesystem: ScoobiAction[FileSystem] =
    ScoobiAction(sc => RIO.safe(FileSystem.get(sc.configuration)))

  def scoobiConfiguration: ScoobiAction[ScoobiConfiguration] =
    ScoobiAction(_.pure[RIO])

  def scoobiJob[A](f: ScoobiConfiguration => A): ScoobiAction[A] = for {
    sc <- ScoobiAction.scoobiConfiguration
    a  <- ScoobiAction.safe(f(sc))
  } yield a

  def log(message: String): ScoobiAction[Unit] =
    ScoobiAction(_ => RIO.putStrLn(message))

  def unless[A](condition: Boolean)(action: ScoobiAction[A]): ScoobiAction[Unit] =
    if (!condition) action.map(_ => ()) else ScoobiAction.ok(())

  implicit def ScoobiActionMonad: Monad[ScoobiAction] = new Monad[ScoobiAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: ScoobiAction[A])(f: A => ScoobiAction[B]) = m.flatMap(f)
  }
}
