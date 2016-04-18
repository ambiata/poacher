package com.ambiata.poacher.mr

import org.specs2._
import org.specs2.matcher.ThrownExpectations
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

  Commit multiple dirs (targets don't exist)

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "path1/f1").write("test1") >> (o /- "path2/f2").write("test2")
         _ <- Committer.commit(c, oc => if (oc == Component("path1")) p /- "p1" else p /- "p2", true)
         a <- (p /- "p1/f1").readOrFail
         b <- (p /- "p2/f2").readOrFail
       } yield a -> b ==== "test1" -> "test2")
     }

  Commit files fails

    ${ prop((h: HdfsTemporary) => (for {
         p <- h.path
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "f1").write("test1")
         _ <- Committer.commit(c, _ => p /- "p1", true)
       } yield ()) must beFail)
     }

  Target dir exists

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         e <- (p /- "p1").mkdirs
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "path1/f1").write("test1")
         _ <- Committer.commit(c, _ => p /- "p1", true)
         a <- (p /- "p1/f1").readOrFail
       } yield e.as(a) ==== "test1".some)
     }

  Target dir exists with hidden files only

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         d = p /- "p1"
         e <- d.mkdirs
         _ <- (d /- ".hidden").write("test")
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "path1/f1").write("test1")
         _ <- Committer.commit(c, _ => d, true)
         a <- (p /- "p1/f1").readOrFail
       } yield e.as(a) ==== "test1".some)
     }

  Target dir exists with non hidden files fails

    ${ prop((h: HdfsTemporary) => (for {
         p <- h.path
         d = p /- "p1"
         _ <- d.mkdirs
         _ <- (d /- "nonhidden").write("test")
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "path1/f1").write("test1")
         _ <- Committer.commit(c, _ => d, true)
       } yield ()) must beFail)
     }

  Commit nested dirs

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         c = MrContext(ContextId.randomContextId)
         o = c.output
         _ <- (o /- "path1/s1/f1").write("test1") >> (o /- "path2/s2/s3/f2").write("test2")
         _ <- Committer.commit(c, oc => if (oc == Component("path1")) p /- "p1" else p /- "p2", true)
         a <- (p /- "p1/s1/f1").readOrFail
         b <- (p /- "p2/s2/s3/f2").readOrFail
       } yield a -> b ==== "test1" -> "test2")
     }

"""

}
