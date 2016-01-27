package com.ambiata.poacher.hdfs

import org.specs2.Specification
import org.apache.hadoop.fs.Path

import com.ambiata.disorder._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.path._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.poacher.hdfs.HdfsMatcher._

import org.apache.hadoop.conf.Configuration

import scala.io.Codec

import org.specs2._
import scalaz._, Scalaz._

class HdfsFileSpec extends Specification with ScalaCheck { def is = s2"""


 HdfsFile
 ========

  A HdfsFile can be created from

    a unsafe String

      ${ HdfsFile.unsafe("hello/world").path.path === "hello/world"  }

    a String

      ${ prop((h: HdfsTemporary) => for {
           f <- h.file
           d <- HdfsFile.fromString(f.path.path)
         } yield d ==== f.some)
       }

    a Uri

      ${ prop((h: HdfsTemporary) => for {
           f <- h.file
           d <- HdfsFile.fromURI(f.toHPath.toUri)
         } yield d ==== f.some)
       }

    a List

      ${ prop((h: HdfsTemporary) => for {
           f <- h.file
           d <- HdfsFile.fromList(Root, f.path.names)
         } yield d ==== f.some)
       }

    get the path as a string

      ${ HdfsFile.unsafe("test").path.path must_== "test" }

  A list of HdfsFile can be ordered

    ${ List(HdfsFile.unsafe("z"), HdfsFile.unsafe("a")).sorted ====
         List(HdfsFile.unsafe("a"), HdfsFile.unsafe("z")) }

  'HdfsFile.toHdfsPath' is symmetric with 'HdfsPath#determineFile'

    ${ prop((h: HdfsTemporary) => for {
         f <- h.file
         d <- f.toHdfsPath.determineFile
       } yield d ==== f)
     }

  'HdfsFile.fromPath' is symmetric with 'HdfsPath#toHPath'

    ${ prop((h: HdfsTemporary) => for {
         f <- h.file
         p = f.toHPath
         d <- HdfsFile.fromPath(p)
       } yield d ==== f.some)
     }

 Operations
 ==========

  HdfsFile should be able to perform these basic operations

    Check that a file exists

      ${ prop((h: HdfsTemporary) => for {
           f <- h.file
           e <- f.exists
         } yield e ==== true)
       }

      ${ prop((h: HdfsTemporary) => for {
           f <- h.file
           _ <- f.delete
           e <- f.exists
         } yield e ==== false)
       }

      ${ prop((h: HdfsTemporary) => (for {
           p <- h.path
           f <- p.write("")
           _ <- f.delete
           _ <- p.mkdirs
           e <- f.exists
         } yield ()) must beFail)
       }

      ${ prop((h: HdfsTemporary) => (for {
           p <- h.path
           _ <- p.mkdirs
           e <- HdfsFile.unsafe(p.path.path).exists
         } yield ()) must beFail)
       }

    On exists operations work as expected

      ${ var i = 0; HdfsTemporary.random.file.flatMap(_.whenExists(Hdfs.io(i = 1))).map(_ => i ==== 1) }

      ${ var i = 0; HdfsTemporary.random.file.flatMap(_.doesExist("", Hdfs.io(i = 1))).map(_ => i ==== 1) }

      ${ var i = 0; HdfsTemporary.random.file.flatMap(_.doesNotExist("", Hdfs.unit)) must beFail }

      ${ var i = 0; HdfsFile.unsafe("test").doesNotExist("", Hdfs.io({ i = 1; i })).map(_ ==== 1) }

  HdfsFile should be able to perform a checksum

    ${ prop((s: S, l: HdfsTemporary) => for {
         f <- l.fileWithContent(s.value)
         r <- f.checksum(MD5)
       } yield r ==== Checksum.string(s.value, MD5).some)
     }

    ${ prop((s: S, l: HdfsTemporary) => for {
         f <- l.fileWithContent(s.value)
         _ <- f.delete
         r <- f.checksum(MD5)
       } yield r ==== None)
     }


  HdfsFile should be able to count the number of lines in a file

    ${ prop((s: List[Int], l: HdfsTemporary) => for {
         p <- l.path
         f <- p.writeLines(s.map(_.toString))
         r <- f.lineCount
       } yield r ==== s.length.some)
     }

    ${ prop((l: HdfsTemporary) => for {
         p <- l.path
         f <- p.write("")
         r <- f.lineCount
       } yield r ==== 0.some)
     }


  HdfsFile should be able to read different content from files using different methods

    Read a string from a file

      ${ prop((s: S, l: HdfsTemporary) => for {
           f <- l.fileWithContent(s.value)
           r <- f.read
         } yield r ==== s.value.some)
       }

    Read a string with a specific encoding from a file

      ${ prop((s: EncodingS, l: HdfsTemporary) => for {
           p <- l.path
           f <- p.writeWithEncoding(s.value, s.codec)
           r <- f.readWithEncoding(s.codec)
         } yield r ==== s.value.some)
       }

    Read lines from a file

      ${ prop((s: List[N], l: HdfsTemporary) => for {
           p <- l.path
           f <- p.writeLines(s.map(_.value))
           r <- f.readLines
         } yield r ==== s.map(_.value).some)
       }

    Read a file using an InputStream

      ${ prop((s: S, l: HdfsTemporary) => {
           var x: String = "";
           for {
             f <- l.fileWithContent(s.value)
             _ <- f.readUnsafe(in => for {
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
             f <- p.writeLines(list.map(_.value))
             r <- f.doPerLine(s =>
               Hdfs.safe({ i += s; () }))
           } yield i.toList ==== list.map(_.value)
         })
       }

    Fold over each line read, keeping an accumulator

      ${ prop((list: List[N], l: HdfsTemporary) => for {
           p <- l.path
           f <- p.writeLines(list.map(_.value))
           r <- f.readPerLine(scala.collection.mutable.ListBuffer[String]())((s, b) => { b +=s; b})
         } yield r.toList ==== list.map(_.value))
       }

    Read lines with a specific encoding from a file

      ${ prop((n: EncodingListN, l: HdfsTemporary) => for {
           p <- l.path
           f <- p.writeLinesWithEncoding(n.value, n.codec)
           r <- f.readLinesWithEncoding(n.codec)
         } yield r ==== n.value.some)
       }

    Read bytes from a file

      ${ prop((bs: Array[Byte], l: HdfsTemporary) => for {
           p <- l.path
           f <- p.writeBytes(bs)
           r <- f.readBytes
         } yield r.map(_.toList) ==== bs.toList.some)
       }

    Read string from a file or fail

      ${ prop((s: S, l: HdfsTemporary) => for {
           f <- l.fileWithContent(s.value)
           r <- f.readOrFail
         } yield r ==== s.value)
       }

    Handle failure cases

     ${ prop((l: HdfsTemporary) => l.file.flatMap(f => f.delete >> f.read).map(_ ==== None)) }

     ${ prop((l: HdfsTemporary) => l.file.flatMap(f => f.delete >> f.readOrFail) must beFail) }


  HdfsFile should be able to append different content to files that exist >> LOL.jokes

  HdfsFile should be able to overwrite content in files

    Overwrite a string in a file that exists and has content

      ${ prop((d: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           f <- p.write(d.first.value)
           _ <- f.overwrite(d.second.value)
           r <- f.read
         } yield r ==== d.second.value.some)
       }

    Overwrite a string in a file that doesn't exist

      ${ prop((d: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           f <- p.write(d.first.value)
           _ <- f.delete
           _ <- f.overwrite(d.second.value)
           r <- f.read
         } yield r ==== d.second.value.some)
       }

     Overwrite a string with a specific encoding in a file that exists and has content

       ${ prop((a: EncodingS, b: EncodingS, l: HdfsTemporary) => for {
            p <- l.path
            f <- p.writeWithEncoding(a.value, a.codec)
            _ <- f.overwriteWithEncoding(b.value, b.codec)
            r <- f.readWithEncoding(b.codec)
          } yield r ==== b.value.some)
        }

     Overwrite a list of strings in a file that exists and has content

       ${ prop((i: S, s: List[N], l: HdfsTemporary) => for {
            p <- l.path
            f <- p.write(i.value)
            _ <- f.overwriteLines(s.map(_.value))
            r <- f.readLines
          } yield r ==== s.map(_.value).some)
        }

     Overwrite a list of strings with a specific encoding in a file that exists and has content

       ${ prop((a: EncodingListN, b: EncodingListN, c: Codec, l: HdfsTemporary) => for {
            p <- l.path
            f <- p.writeLinesWithEncoding(a.value, a.codec)
            _ <- f.overwriteLinesWithEncoding(b.value, b.codec)
            r <- f.readLinesWithEncoding(b.codec)
          } yield r ==== b.value.some)
        }

     Overwrite bytes in a file that exists and has content

       ${ prop((i: Array[Byte], s: Array[Byte], l: HdfsTemporary) => for {
            p <- l.path
            f <- p.writeBytes(i)
            _ <- f.overwriteBytes(s)
            r <- f.readBytes
          } yield r.map(_.toList) ==== s.toList.some)
        }

  HdfsFile should be able to move files

    ${ prop((s: S, l: HdfsTemporary) => for {
         p <- l.path
         f <- l.fileWithContent(s.value)
         n <- f.move(p)
         e <- f.exists
         r <- n.readOrFail
       } yield e -> r ==== false -> s.value)
     }

    Move with mode and overwrite the target file

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           n <- f.moveWithMode(p, TargetMode.Overwrite)
           r <- n.readOrFail
         } yield r ==== s.value)
       }

    Move a file to a directory checking the path and contents

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           f <- l.fileWithContent(s.value)
           n <- f.moveTo(d)
           a <- f.exists
           r <- n.readOrFail
         } yield a -> r ==== false -> s.value)
       }

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           f <- p.write(s.value)
           n <- f.moveTo(d)
         } yield n.toHdfsPath ==== (d.toHdfsPath | p.basename.get))
       }

    Moving a file can fail, handle those failure cases.

      ${ prop((s: S, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           _ <- f.delete
           _ <- f.move(p)
         } yield ()) must beFail)
       }

      ${ prop((s: S, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           _ <- f.move(p)
         } yield ()) must beFail)
       }

  HdfsFile should be able to copy files and handle copy failures

    Copying a file should leave the original file intact

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           f <- l.fileWithContent(s.value)
           n <- f.copy(p)
           e <- f.exists
           r <- n.readOrFail
         } yield e -> r ==== true -> s.value)
       }

    Copying a file to a directory, should put the file inside the directory

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           f <- l.fileWithContent(s.value)
           n <- f.copyTo(d)
           a <- f.exists
           r <- n.readOrFail
         } yield a -> r ==== true -> s.value)
       }

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           f <- p.write(s.value)
           n <- f.copyTo(d)
         } yield n.toHdfsPath ==== (d.toHdfsPath | p.basename.get))
       }

    Copying a file with the 'Overwrite' mode should not fail when the target exists

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           n <- f.copyWithMode(p, TargetMode.Overwrite)
           r <- n.readOrFail
         } yield r ==== s.value)
       }

    Copying a file with the 'Fail' mode should fail when the target exists

      ${ prop((s: S, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           n <- f.copyWithMode(p, TargetMode.Fail)
         } yield ()) must beFail)
       }

    Copying a file should fail when the souce no longer exists

      ${ prop((s: S, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           _ <- f.delete
           _ <- f.copy(p)
         } yield ()) must beFail)
       }

    Copying a file should fail when the target exists

      ${ prop((s: S, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write("")
           f <- l.fileWithContent(s.value)
           _ <- f.copy(p)
        } yield ()) must beFail)
       }


"""

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
