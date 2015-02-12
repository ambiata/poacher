package com.ambiata.poacher.mr

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.path._

import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.poacher.hdfs.HdfsMatcher._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._

class CommitterSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

Committer
-----------

  Commit multiple dirs (targets don't exist)    $e1
  Commit files fails                            e2
  Target dir exists                             exists
  Target dir exists with hidden files only      hidden
  Commit nested dirs                            nestedDir

"""

  def e1 = prop((h: HdfsTemporary) => for {
    p <- h.path
    c = MrContext(ContextId.randomContextId)
    o = c.output
    _ <- (o /- "path1/f1").write("test1") >> (o /- "path2/f2").write("test2")
    _ <- Committer.commit(c, oc => if (oc == "path1".some) p /- "p1" else p /- "p2", true)
    a <- (p /- "p1/f1").readOrFail
    b <- (p /- "p2/f2").readOrFail
  } yield a -> b ==== "test1" -> "test2")


  def doer(output: HdfsPath => Hdfs[Unit], dest: Option[Component] => HdfsPath, expected: List[(HdfsPath, String)]) = {
    val t = MrContext(ContextId.randomContextId)
    for {
      _ <- output(t.output)
      _ <- Committer.commit(t, dest, true)
      _ <- expected.traverse({ case (p, e) => p.read.map(_ must_== e) })
    } yield ()
  }

/*
  def e1 =
    commit((ctx, _) =>
      (HdfsPath.fromPath(ctx.output) /- "path1/f1").write("test1") >>
        (HdfsPath.fromPath(ctx.output) /- "path2/f2").write("test2").void,
      (target, p) => if (p == "path1") new Path(target, "p1") else new Path(target, "p2"),
      List("p1/f1" -> "test1", "p2/f2" -> "test2")
    ).toEither must beRight

  def e2 =
    commit((ctx, _) => (HdfsPath.fromPath(ctx.output) /- "f1").write("test1").void,
      (target, _) => target, Nil).toEither must beLeft

  def exists =
    commit((ctx, target) =>
      HdfsPath.fromPath(target).mkdirs.void >>
        (HdfsPath.fromPath(ctx.output) /- "path1/f1").write("test1") >>
          (HdfsPath.fromPath(ctx.output) /- "path1/f1").write("test1").void,
      (target, _) => target, List("f1" -> "test1")
    ).toEither must beLeft

  def hidden =
    commit((ctx, target) =>
      HdfsPath.fromPath(target).mkdirs.void >>
        (HdfsPath.fromPath(ctx.output) /- "path1/.f1").write("test1").void,
      (target, _) => target, nil
    ).toEither must beRight

  def nestedDir =
    commit((ctx, _) =>
      (HdfsPath.fromPath(ctx.output) /- "path1/f1/f2").write("test1").void,
      (target, _) => new Path(target, "p1"), List("p1/f1/f2" -> "test1")
    ).toEither must beRight

  private def commit(pre: (MrContext, Path) => Hdfs[Unit],
                     mapping: (Path, String) => Path,
                     expected: List[(String, String)]): Result[Unit] = (for {
    c <- ConfigurationTemporary.random.conf
    t = MrContext(ContextId.randomContextId)
    _ <- (for {
      p <- HdfsTemporary.random.path
      _ <- pre(t, p.toHPath)
      _ <- Committer.commit(t, mapping(p.toHPath, _), cleanup = true)
      _ <- expected.traverse({ case (path, e) => (p /- path).read.map(_ must_== e) })
    } yield ()).run(c)
  } yield ()).unsafePerformIO
*/
}
