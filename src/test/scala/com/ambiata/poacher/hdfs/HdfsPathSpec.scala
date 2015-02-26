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
import org.specs2.matcher.DisjunctionMatchers
import scalaz._, Scalaz._, effect.Effect._

class HdfsPathSpec extends Specification with ScalaCheck with DisjunctionMatchers { def is = s2"""


 HdfsPath
 ========

  HdfsPath operations should have the symenatics as Path operations

    ${ prop((l: Path, p: Path) => (HdfsPath(l) / p).path ==== l / p) }

    ${ prop((l: Path, p: Path) => (HdfsPath(l).join(p)).path ==== l.join(p)) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l) | p).path ==== (l | p)) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l).extend(p)).path ==== l.extend(p)) }

    ${ prop((l: Path, p: S) => (HdfsPath(l) /- p.value).path ==== l /- p.value) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l) | p).rebaseTo(HdfsPath(l)).map(_.path) ==== (l | p).rebaseTo(l)) }

    ${ prop((l: Path) => HdfsPath(l).dirname.path ==== l.dirname) }

    ${ prop((l: Path) => HdfsPath(l).basename ==== l.basename) }

  A list of HdfsPath can be ordered

    ${ List(HdfsPath.fromString("z"), HdfsPath.fromString("a")).sorted ====
         List(HdfsPath.fromString("a"), HdfsPath.fromString("z")) }

 HdfsPath IO
 ===========

  HdfsPath should be able to determine files, directories and handle failure cases

    ${ HdfsTemporary.random.path.flatMap(path => path.touch >> path.determine.map(_ must beFile)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.determine.map(_ must beDirectory)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.determine.map(_ must beNone)) }

    ${ HdfsPath(Path("empty")).determine.map(_ must beNone) }

  HdfsPath can determine a file and handle failure cases

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         f <- p.write("")
         r <- p.determineFile
       } yield r ==== f)
     }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.determineFile) must beFail }

    ${ HdfsTemporary.random.path.flatMap(path => path.determineFile) must beFail }

  HdfsPath can determine a directory and handle failure cases

    ${ HdfsTemporary.random.path.flatMap(path => path.touch >> path.determineDirectory) must beFail }

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         f <- p.mkdirs
         r <- p.determineDirectory
       } yield r ==== f)
     }

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         _ <- p.mkdirs
         r <- p.isDirectory
       } yield r ==== true)
     }

    ${ HdfsTemporary.random.path.flatMap(path => path.determineDirectory) must beFail }

  HdfsPath should be able to perform these basic operations

    Check if a path exists

      ${ prop((l: HdfsTemporary) => l.path.flatMap(p => p.touch >> p.exists.map(_ ==== true))) }

      ${ prop((l: HdfsTemporary) => l.path.flatMap(p => p.mkdirs >> p.exists.map(_ ==== true))) }

      ${ prop((l: HdfsTemporary) => l.path.flatMap(_.exists.map(_ ==== false))) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(p => p.touch >> p.doesExist("",
           Hdfs.io(i = 1)).map(_ => i ==== 1)) }) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(p => p.touch >>
           p.whenExists(Hdfs.io(i = 1)).as(i).map(_ => i ==== 1)) }) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(_.doesNotExist("",
           Hdfs.io({ i = 1; i })).map(_ ==== 1)) }) }

    Delete a path

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.touch
           b <- p.exists
           _ <- p.delete
           r <- p.exists
         } yield b -> r ==== true -> false)
       }

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.mkdirs
           b <- p.exists
           _ <- p.delete
           r <- p.exists
         } yield b -> r ==== true -> false)
       }

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.delete
         } yield ()) must beFail)
       }


  HdfsFile should be able to touch files which will update the last modified time, but not affect the content.

    ${ prop((v: S, l: HdfsTemporary) => for {
         p <- l.path
         _ <- p.write(v.value)
         _ <- p.touch
         r <- p.readOrFail
       } yield r ==== v.value)
     }

    ${ prop((l: HdfsTemporary) => for {
         p <- l.path
         f <- p.write("")
         b <- f.lastModified
         _ <- Hdfs.safe(Thread.sleep(1100))
         _ <- p.touch
         a <- f.lastModified
       } yield b must be_<(a)).set(minTestsOk = 3)
     }

  HdfsPath should be able to perform a checksum

    ${ prop((s: S, l: HdfsTemporary) => for {
         p <- l.path
         _ <- p.write(s.value)
         r <- p.checksum(MD5)
       } yield r ==== Checksum.string(s.value, MD5).some)
     }

    ${ prop((s: S, l: HdfsTemporary) => for {
         p <- l.path
         _ <- p.write(s.value)
         _ <- p.delete
         r <- p.checksum(MD5)
       } yield r ==== None)
     }

  HdfsPath should be able to count the number of lines in a file

    ${ prop((s: List[Int], l: HdfsTemporary) => for {
         p <- l.path
         _ <- p.writeLines(s.map(_.toString))
         r <- p.lineCount
       } yield r ==== s.length.some)
     }

    ${ prop((s: S, l: HdfsTemporary) => for {
         p <- l.path
         r <- p.lineCount
       } yield r ==== None)
     }

  HdfsPath should be able to read and write to a file. These operations should be symmetrical

    Write a string to a file and read it back

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(s.value)
           r <- p.readOrFail
         } yield r ==== s.value)
       }

      ${ prop((v: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(v.value)
           r <- p.read
         } yield r ==== v.value.some)
       }

    Read and write a string with a specific encoding

      ${ prop((v: EncodingN, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeWithEncoding(v.value, v.codec)
           r <- p.readWithEncoding(v.codec)
         } yield r ==== v.value.some)
       }

    Read and write a lines to a file

      ${ prop((v: List[N], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLines(v.map(_.value))
           r <- p.readLines
         } yield r ==== v.map(_.value).some)
       }

    Read and write lines with a specific encoding to a file

      ${ prop((v: EncodingListN, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLinesWithEncoding(v.value, v.codec)
           r <- p.readLinesWithEncoding(v.codec)
         } yield r ==== v.value.some)
       }

    Read a file using an InputStream

      ${ prop((s: S, l: HdfsTemporary) => {
           var x: String = "";
           for {
             p <- l.path
             _ <- p.write(s.value)
             _ <- p.readUnsafe(in => for {
               v <- Hdfs.fromRIO(Streams.read(in))
               _ <- Hdfs.safe(x = v)
             } yield ())
           } yield x ==== s.value
         })
       }

    Run a function for every line read in as a String

      ${ prop((list: List[N], l: HdfsTemporary) => {
           var i = scala.collection.mutable.ListBuffer[String]()
           for {
             p <- l.path
             _ <- p.writeLines(list.map(_.value))
             r <- p.doPerLine(s =>
               Hdfs.safe({ i += s; () }))
           } yield i.toList ==== list.map(_.value)
         })
       }

    Fold over each line read, keeping an accumulator

      ${ prop((list: List[N], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLines(list.map(_.value))
           r <- p.readPerLine(scala.collection.mutable.ListBuffer[String]())((s, b) => { b +=s; b})
         } yield r.toList ==== list.map(_.value))
       }

    Read lines with a specific encoding from a file

      ${ prop((v: EncodingListN, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLinesWithEncoding(v.value, v.codec)
           r <- p.readPerLineWithEncoding(v.codec, scala.collection.mutable.ListBuffer[String]())((s, b) => { b +=s; b})
         } yield r.toList ==== v.value)
       }

    Read and write bytes to a file

      ${ prop((bs: Array[Byte], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeBytes(bs)
           r <- p.readBytes
         } yield r.map(_.toList) ==== bs.toList.some)
       }

    Handle failure cases
nhibberd
      { prop((l: HdfsTemporary) => l.path.flatMap(_.read) must beOkLike(_ must beNone)) }

      ${ prop((l: HdfsTemporary) => l.path.flatMap(_.readOrFail) must beFail) }



"""
  val beFile = beSome(be_-\/[HdfsFile])
  val beDirectory = beSome(be_\/-[HdfsDirectory])

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
