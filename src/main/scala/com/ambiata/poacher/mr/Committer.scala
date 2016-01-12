package com.ambiata.poacher.mr

import com.ambiata.mundane.path._
import com.ambiata.poacher.hdfs._

import scalaz._, Scalaz._

/**
 * Move tmp output to final locations on hdfs
 */
object Committer {

  /**
   * Move the children of each dir under context.output to the directory specified by the mapping
   * function.
   *
   * The mapping function takes the name of the dir under context.output and must return
   * a destination directory. If the destination dir doesn't exist, it will be created.
   * If the destination dir or any of the children in context.output is a file, an error
   * will be returned.
   */
  def commit(context: MrContext, mapping: Option[Component] => HdfsPath, cleanup: Boolean): Hdfs[Unit] = for {
    g <- context.output.globPaths("*")
    h =  HdfsPath.filterHidden(g)
    _ <- h.traverse(p => for {
      n <- Hdfs.ok(mapping(p.basename))
      e <- n.exists
      l <- if (e) n.globPaths("*")
           else   nil.pure[Hdfs]
      t =  HdfsPath.filterHidden(l)
      _ <- Hdfs.unless(t.isEmpty)(Hdfs.fail(s"Attempting to commit to a path that already contains data: '${n}'"))
      c <- p.isDirectory
      _ <- if (!c) Hdfs.fail(s"Can not commit '${p}' as its not a dir") else Hdfs.ok(())
      d <- n.mkdirsOrFail
      s <- p.globPaths("*")
      _ <- s.traverse(subpath => subpath.move(d.toHdfsPath))
    } yield ())
    _ <- Hdfs.when(cleanup)(context.cleanup)
  } yield ()

}
