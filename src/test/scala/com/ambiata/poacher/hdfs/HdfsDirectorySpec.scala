package com.ambiata.poacher.hdfs

import org.specs2.Specification

import com.ambiata.disorder._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.path._
import com.ambiata.mundane.path.Arbitraries._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.poacher.hdfs.HdfsMatcher._

import org.apache.hadoop.conf.Configuration

import scala.io.Codec

import org.specs2._
import scalaz._, Scalaz._, effect.Effect._

class HdfsDirectorySpec extends Specification with ScalaCheck { def is = s2"""


 HdfsDirectory
 =============

  A HdfsDirectory can be created from

    a unsafe String

      ${ HdfsDirectory.unsafe("hello/world").path.path === "hello/world"  }

    a String

      ${ prop((h: HdfsTemporary) => for {
           f <- h.directory
           d <- HdfsDirectory.fromString(f.path.path)
         } yield d ==== f.some)
       }

    a Uri

      ${ prop((h: HdfsTemporary) => for {
           f <- h.directory
           d <- HdfsDirectory.fromURI(f.toHPath.toUri)
         } yield d ==== f.some)
       }

    a List

      ${ prop((h: HdfsTemporary) => for {
           f <- h.directory
           d <- HdfsDirectory.fromList(Root, f.path.names)
         } yield d ==== f.some)
       }

    get the path as a string

      ${ HdfsDirectory.unsafe("test").path.path must_== "test" }

  HdfsDirectory can return it's parent

    ${ HdfsDirectory.unsafe("test").parent.map(_.path) ==== Relative.some }

    ${ HdfsDirectory.unsafe("/test").parent ==== HdfsDirectory.unsafe("/").some }

  A list of HdfsDirectory can be ordered

    ${ List(HdfsDirectory.unsafe("z"), HdfsDirectory.unsafe("a")).sorted ====
         List(HdfsDirectory.unsafe("a"), HdfsDirectory.unsafe("z")) }

  'HdfsDirectory.toHdfsPath' is symmetric with 'HdfsPath#determineDirectory'

    ${ prop((h: HdfsTemporary) => for {
         f <- h.directory
         d <- f.toHdfsPath.determineDirectory
       } yield d ==== f)
     }

  'HdfsDirectory.fromPath' is symmetric with 'HdfsPath#toHPath'

    ${ prop((h: HdfsTemporary) => for {
         f <- h.directory
         p = f.toHPath
         d <- HdfsDirectory.fromPath(p)
       } yield d ==== f.some)
     }

 Operations
 ==========

  HdfsDirectory should be able to perform these basic operations

    Check that a directory exists

      ${ prop((h: HdfsTemporary) => for {
           d <- h.directory
           e <- d.exists
         } yield e ==== true)
       }

    Can delete a directory

      ${ prop((h: HdfsTemporary) => for {
           d <- h.directory
           _ <- d.delete
           e <- d.exists
         } yield e ==== false)
       }

    Can create a directory

      ${ prop((h: HdfsTemporary) => for {
           d <- h.directory
           _ <- d.delete
           _ <- d.mkdirs
           e <- d.exists
         } yield e ==== true)
       }

    Can handle failure when nothing exists or a file exists

      ${ prop((h: HdfsTemporary) => (for {
           p <- h.path
           d <- p.mkdirs
           _ <- d.delete
           _ <- p.write("")
           e <- d.exists
         } yield ()) must beFail)
       }

      ${ prop((h: HdfsTemporary) => (for {
           p <- h.path
           _ <- p.write("")
           e <- HdfsDirectory.unsafe(p.path.path).exists
         } yield ()) must beFail)
       }

    On exists operations work as expected

      ${ var i = 0; HdfsTemporary.random.directory.flatMap(_.whenExists(Hdfs.io(i = 1))).map(_ => i ==== 1) }

      ${ var i = 0; HdfsTemporary.random.directory.flatMap(_.doesExist("", Hdfs.io(i = 1))).map(_ => i ==== 1) }

      ${ var i = 0; HdfsTemporary.random.directory.flatMap(_.doesNotExist("", Hdfs.unit)) must beFail }

      ${ var i = 0; HdfsDirectory.unsafe("test").doesNotExist("", Hdfs.io({ i = 1; i })).map(_ ==== 1) }


  HdfsDirectory should be able to calculate size

    Calculate the size of a directory with only files

      ${ prop((v: DistinctPair[Component], a: S, h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first).write(a.value)
           r <- d.sizeOrFail
         } yield r ==== a.value.getBytes.length.bytes)
       }

    Calculate the size of a directory with files and sub-directories

      ${ prop((v: DistinctPair[Component], a: S, b: S, h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).write(a.value)
           _ <- (d.toHdfsPath | v.second).write(b.value)
           r <- d.size
         } yield r ==== (a.value.getBytes.length + b.value.getBytes.length).bytes.some)
       }


  HdfsDirectory should be able to count the number of files

    Calculate the number of files in a directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.numberOfFiles
         } yield r ==== 2.some)
       }

    Calculate the number of files in a directory with sub-directories

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.first | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.first | v.first | v.first).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.numberOfFiles
         } yield r ==== 4.some)
       }


  HdfsDirectory should be able to move a directory handling failure cases gracefully

    Move a empty directory to a path

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           n <- d.move(p)
           b <- d.exists
           a <- n.exists
         } yield b -> a ==== false -> true)
       }

    Move a directory with multiple sub-directories and files to a path

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           p <- h.path
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.first | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.second).touch
           n <- d.move(p)
           r <- n.listPathsRecursively.map(_.map(_.path.rebaseTo(d.path)))
           e <- (n.toHdfsPath | v.first | v.first | v.second).exists
         } yield r.size -> e ==== 5 -> true)
       }

    Move a directory to a directory that already exists should fail

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.directory
           d <- l.directory
           n <- d.move(p.toHdfsPath)
         } yield ()) must beFail)
       }

    Trying to move a directory that does not exist should fail

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.path
           d =  HdfsDirectory.unsafe("test")
           n <- d.move(p)
         } yield ()) must beFail)
       }

    Trying to move '/' (root) should fail

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.path
           d =  HdfsDirectory.unsafe("/")
           n <- d.move(p)
         } yield ()) must beFail)
       }

  HdfsDirectory should be able to move a directory with respect to different TargetMode's

    Move a directory to a directory that already exists, overwrite the destination directory

      ${ prop((l: HdfsTemporary) => for {
           p <- l.directory
           d <- l.directory
           n <- d.moveWithMode(p.toHdfsPath, TargetMode.Overwrite)
           b <- d.exists
           a <- n.exists
         } yield b -> a ==== false -> true)
       }

    Move a directory to a directory that already exists should fail

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.directory
           d <- l.directory
           n <- d.moveWithMode(p.toHdfsPath, TargetMode.Fail)
         } yield ()) must beFail)
       }

  HdfsDirectory should be able to move a directory to another directory

    Place the original directory inside the target directory

      ${ prop((l: HdfsTemporary) => for {
           p <- l.directory
           d <- l.directory
           n <- d.moveTo(p)
           z <- d.exists
           r <- n.exists
         } yield z -> r ==== false -> true)
       }

      ${ prop((l: HdfsTemporary) => for {
           p <- l.directory
           d <- l.directory
           n <- d.moveTo(p)
         } yield n.toHdfsPath ==== (p.toHdfsPath | d.path.basename.get))
       }


  HdfsDirectory should be able to list files

    List files in directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           f <- (d.toHdfsPath | v.first).write("")
           _ <- (d.toHdfsPath | v.second).mkdirs
           r <- d.listFiles
         } yield r ==== List(f))
       }

    List files relative to the directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first).touch
           _ <- (d.toHdfsPath | v.second).mkdirs
           r <- d.listFilesRelativeTo.map(_.map(_._2.basename))
         } yield r ==== List(v.first.some))
       }

    List files recursively relative to the directory

      ${ prop((v: Component, h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v | v).touch
           r <- d.listFilesRecursivelyRelativeTo.map(_.map(_._2))
         } yield r ==== List(HdfsPath(Path(v.name) | v)))
       }

  HdfsDirectory should be able to list directories

    List directories in directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first).touch
           e <- (d.toHdfsPath | v.second).mkdirs
           r <- d.listDirectories
         } yield r ==== List(e))
       }

    List directories relative to the directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.listDirectories.map(_.map(_.toHdfsPath.basename))
         } yield r ==== List(v.first.some))
       }

    List directories recursively relative to the directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.first | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.listDirectoriesRecursively.map(_.map(_.path.rebaseTo(d.path)))
         } yield r ==== List(Path(v.first.name).some, (Path(v.first.name) | v.first).some))
       }

  HdfsDirectory should be able to list paths which represent both files and directories

    List paths in the given directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.listPaths.map(_.map(_.path.rebaseTo(d.path)))
         } yield r.sorted ==== List(Path(v.second.name), Path(v.first.name)).map(_.some).sorted)
       }

    List paths recrusively

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           d <- h.directory
           _ <- (d.toHdfsPath | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.first | v.first | v.second).touch
           _ <- (d.toHdfsPath | v.second).touch
           r <- d.listPathsRecursively.map(_.map(_.path.rebaseTo(d.path)))
         } yield r.sorted ==== List(Path(v.first.name), Path(v.second.name), Path(v.first.name) | v.second,
            Path(v.first.name) | v.first, Path(v.first.name) | v.first | v.second).map(_.some).sorted)
       }


    ...



"""
}
