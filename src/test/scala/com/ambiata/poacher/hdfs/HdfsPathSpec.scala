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

import java.net.URI

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

  A HdfsPath can be created from

    a String

      ${ HdfsPath.fromString("hello/world").path.path === "hello/world" }

    a Path

      ${ HdfsPath(Path("hello/world")).path === Path("hello/world") }

    a Uri

      ${ HdfsPath.fromURI(new URI("hello/world")).map(_.path.path) === "hello/world".some }

      ${ HdfsPath.fromURI(new URI("hdfs:///hello/world")).map(_.path.path) === "/hello/world".some }

      ${ HdfsPath.fromURI(new URI("s3:///hello/world")) must beNone }


  A list of HdfsPath can be ordered

    ${ List(HdfsPath.fromString("z"), HdfsPath.fromString("a")).sorted ====
         List(HdfsPath.fromString("a"), HdfsPath.fromString("z")) }

 HdfsPath IO
 ===========

  HdfsPath should be able to determine files, directories and handle failure cases

    ${ HdfsTemporary.random.path.flatMap(path => path.touch >> path.determine.map(_ must beFile)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.determine.map(_ must beDirectory)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.determine.map(_ must beNone)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.touchDetermine.map(_.swap must be_-\/[HdfsDirectory])) }

    ${ HdfsTemporary.random.path.flatMap(path => path.touchDetermine.map(_ must be_-\/[HdfsFile])) }

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
         f <- p.mkdirsOrFail
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


  HdfsPath should be able to calculate the size of files/directories/paths

    Size of a single file

      ${ prop((v: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(v.value)
           r <- p.sizeOrFail
         } yield r ==== v.value.getBytes.length.bytes)
       }

    Size of a directory

      ${ prop((f: DistinctPair[Component], v: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           _ <- (p | f.first).write(v.first.value)
           _ <- (p | f.second).write(v.second.value)
           r <- p.size
         } yield r ==== (v.first.value.getBytes.length + v.second.value.getBytes.length).bytes.some)
       }


  HdfsPath should be able to count the number of files within a path

    A single file

      ${ prop((h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.write("")
           r <- p.numberOfFiles
         } yield r ==== 1.some)
       }

    Calculate the number of files in a directory

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.mkdirs
           _ <- (p | v.first).touch
           _ <- (p | v.second).touch
           r <- p.numberOfFiles
         } yield r ==== 2.some)
       }

    Calculate the number of files in a directory with sub-directories

      ${ prop((v: DistinctPair[Component], h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.mkdirs
           _ <- (p | v.first | v.second).touch
           _ <- (p | v.first | v.first | v.second).touch
           _ <- (p | v.first | v.first | v.first).touch
           _ <- (p | v.second).touch
           r <- p.numberOfFiles
         } yield r ==== 4.some)
       }


  HdfsPath should be able to create directories

    Create a directory successfully

      ${ prop((h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.mkdirs
           r <- p.isDirectory
         } yield r ==== true)
       }

    What happens when a directory already exists

      ${ prop((h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.mkdirs
           e <- p.mkdirs
         } yield !e.isEmpty ==== true)
       }

    Handle failure

      ${ prop((h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.write("")
           e <- p.mkdirs
         } yield e.isEmpty ==== true)
       }

    Create a directory with retry

      ${ prop((c: Component, h: HdfsTemporary) => for {
           p <- h.path
           d <- p.mkdirsWithRetry(c.name, _ => sys.error("invariant"))
         } yield d.map(_.toHdfsPath) ==== (p | c).some)
       }

      ${ prop((c: Component, h: HdfsTemporary) => for {
           p <- h.path
           _ <- p.mkdirsWithRetry(c.name, _ => sys.error("invariant"))
           r <- p.exists
         } yield r ==== true)
       }

     ${ prop((c: Component, n: Int, h: HdfsTemporary) => (n > 0) ==> (for {
           p <- h.path
           _ <- (p | c).mkdirs
           d <- p.mkdirsWithRetry(c.name, _ => n.toString.some)
           v = p /- n.toString
           r <- v.exists
         } yield d.map(_.toHdfsPath) -> r ==== v.some -> true))
       }

     ${ prop((h: HdfsTemporary) => {
           var i = 0
           for {
             p <- h.path
             _ <- (p /- i.toString).mkdirs
             _ = { i += 1 }
             _ = println(s"first: $i")
             _ <- (p /- i.toString).mkdirs
             _ <- p.mkdirsWithRetry(0.toString, _ => { i += 1; println(s"loop: $i"); i.toString.some })
             r <- (p /- i.toString).exists
           } yield r -> i ==== true -> 2 })
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

    Write with an output stream

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeWith(o => Hdfs.fromRIO(Streams.write(o, s.value)))
           r <- p.readOrFail
         } yield r ==== s.value)
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

    Write stream

      ${ prop((s: S, l: HdfsTemporary) => for {
           p <- l.path
           a <- p.write(s.value)
           b <- l.path
           _ <- Hdfs.using(a.toInputStream)(in => b.writeStream(in))
           r <- b.readOrFail
         } yield r ==== s.value)
       }

  HdfsPath should be able to write to files with different modes

    Can write a string to files with different modes

      ${ prop((s: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(s.first.value)
           _ <- p.writeWithMode(s.second.value, HdfsWriteMode.Overwrite)
           r <- p.readOrFail
         } yield r ==== s.second.value)
       }

      ${ prop((s: DistinctPair[S], l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write(s.first.value)
           _ <- p.writeWithMode(s.second.value, HdfsWriteMode.Fail)
         } yield ()) must beFail)
       }

    Can write a string to files with different modes using different encodings

      ${ prop((s: DistinctPair[EncodingS], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(s.first.value)
           _ <- p.writeWithEncodingMode(s.second.value, s.second.codec, HdfsWriteMode.Overwrite)
           r <- p.readWithEncoding(s.second.codec)
         } yield r ==== s.second.value.some)
       }

      ${ prop((s: DistinctPair[EncodingS], l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.write(s.first.value)
           _ <- p.writeWithEncodingMode(s.second.value, s.second.codec, HdfsWriteMode.Fail)
         } yield ()) must beFail)
       }

    Can write lines to a file with different modes

      ${ prop((a: List[N], b: List[N], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLines(a.map(_.value))
           _ <- p.writeLinesWithMode(b.map(_.value), HdfsWriteMode.Overwrite)
           r <- p.readLines
         } yield r ==== (b.map(_.value).some))
       }

      ${ prop((a: List[N], b: List[N], l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.writeLines(a.map(_.value))
           _ <- p.writeLinesWithMode(b.map(_.value), HdfsWriteMode.Fail)
         } yield ()) must beFail)
       }

    Can write lines with different Codec's and Mode's

      ${ prop((a: EncodingListN, b: EncodingListN, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLinesWithEncoding(a.value, a.codec)
           _ <- p.writeLinesWithEncodingMode(b.value, b.codec, HdfsWriteMode.Overwrite)
           r <- p.readLinesWithEncoding(b.codec)
         } yield r ==== b.value.some)
       }

      ${ prop((a: EncodingListN, b: EncodingListN, l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.writeLinesWithEncoding(a.value, a.codec)
           _ <- p.writeLinesWithEncodingMode(b.value, b.codec, HdfsWriteMode.Fail)
         } yield ()) must beFail)
       }

    Can write bytes with different Mode's

      ${ prop((a: Array[Byte], b: Array[Byte], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeBytes(a)
           _ <- p.writeBytesWithMode(b, HdfsWriteMode.Overwrite)
           r <- p.readBytes
         } yield r.map(_.toList) ==== b.toList.some)
       }

      ${ prop((a: Array[Byte], l: HdfsTemporary) => (for {
           p <- l.path
           _ <- p.writeBytes(a)
           _ <- p.writeBytesWithMode(a, HdfsWriteMode.Fail)
         } yield ()) must beFail)
       }


  HdfsPath should be able to overwrite different content to files

    Overwrite a string in a file that exists and has content

      ${ prop((d: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(d.first.value)
           _ <- p.overwrite(d.second.value)
           r <- p.readOrFail
         } yield r ==== d.second.value)
       }

    Overwrite a string in a file that doesn't exist

      ${ prop((d: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(d.first.value)
           _ <- p.delete
           _ <- p.overwrite(d.second.value)
           r <- p.read
         } yield r ==== d.second.value.some)
       }

    Overwrite a string with a specific encoding in a file that exists and has content

      ${ prop((a: EncodingS, b: EncodingS, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeWithEncoding(a.value, a.codec)
           _ <- p.overwriteWithEncoding(b.value, b.codec)
           r <- p.readWithEncoding(b.codec)
         } yield r ==== b.value.some)
       }

    Overwrite a list of strings in a file that exists and has content

      ${ prop((i: S, s: List[N], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(i.value)
           _ <- p.overwriteLines(s.map(_.value))
           r <- p.readLines
         } yield r ==== s.map(_.value).some)
       }

    Overwrite a list of strings with a specific encoding in a file that exists and has content

      ${ prop((a: EncodingListN, b: EncodingListN, c: Codec, l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeLinesWithEncoding(a.value, a.codec)
           _ <- p.overwriteLinesWithEncoding(b.value, b.codec)
           r <- p.readLinesWithEncoding(b.codec)
         } yield r ==== b.value.some)
       }

    Overwrite bytes in a file that exists and has content

      ${ prop((i: Array[Byte], s: Array[Byte], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.writeBytes(i)
           _ <- p.overwriteBytes(s)
           r <- p.readBytes
         } yield r.map(_.toList) ==== s.toList.some)
       }

    Overwrite stream

      ${ prop((s: DistinctPair[S], l: HdfsTemporary) => for {
           p <- l.path
           _ <- p.write(s.first.value)
           b <- l.fileWithContent(s.second.value)
           _ <- Hdfs.using(p.toInputStream)(in => b.toHdfsPath.overwriteStream(in))
           r <- b.readOrFail
         } yield r ==== s.first.value)
       }

  HdfsPath should be able to move files/directories/paths

    Move a single file to a path

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           d <- l.path
           _ <- p.touch
           _ <- p.move(d)
           b <- p.exists
           a <- d.exists
         } yield b -> a ==== false -> true)
       }

    Move a single file to a directory

      ${ prop((v: Component, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           _ <- (p | v).touch
           _ <- (p | v).move(d.toHdfsPath)
           b <- (p | v).exists
           a <- (d.toHdfsPath | v).exists
         } yield b -> a ==== false -> true)
       }

    Move a single file to a file that exists should fail

      ${ prop((v: Component, l: HdfsTemporary) => (for {
           p <- l.path
           d <- l.path
           _ <- (p | v).touch
           _ <- d.touch
           _ <- p.move(d)
         } yield ()) must beFail)
       }

    Move a directory to a path

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           d <- l.path
           _ <- p.mkdirs
           _ <- p.move(d)
           b <- p.exists
           a <- d.exists
         } yield b -> a ==== false -> true)
       }

    Move a directory to a directory

      ${ prop((v: DistinctPair[Component], l: HdfsTemporary) => for {
           p <- l.path
           d <- l.path
           _ <- (p | v.first | v.second).touch
           _ <- d.mkdirs
           _ <- (p | v.first).move(d)
           b <- (p | v.first).exists
           a <- (d | v.first | v.second).exists
         } yield b -> a ==== false -> true)
       }

    Move a directory to a file that exists should fail

      ${ prop((v: Component, l: HdfsTemporary) => (for {
           p <- l.path
           d <- l.path
           _ <- p.mkdirs
           _ <- d.touch
           _ <- p.move(d)
         } yield ()) must beFail)
       }


  HdfsPath should be able to copy files/directories/paths

    Copy a single file to a path

      ${ prop((l: HdfsTemporary) => for {
           p <- l.path
           d <- l.path
           _ <- p.touch
           _ <- p.copy(d)
           b <- p.exists
           a <- d.exists
         } yield b -> a ==== true -> true)
       }

    Copy a single file to a directory

      ${ prop((v: Component, l: HdfsTemporary) => for {
           p <- l.path
           d <- l.directory
           _ <- (p | v).touch
           _ <- (p | v).copy(d.toHdfsPath)
           b <- (p | v).exists
           a <- (d.toHdfsPath | v).exists
         } yield b -> a ==== true -> true)
       }

    Copy a single file to a file that exists should file

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.path
           d <- l.path
           _ <- p.touch
           _ <- d.touch
           _ <- p.copy(d)
         } yield ()) must beFail)
       }

    Copy a directory to a path should be an unsupported operation

      ${ prop((l: HdfsTemporary) => (for {
           p <- l.path
           d <- l.path
           _ <- p.mkdirs
          _ <- p.copy(d)
         } yield ()) must beFail)
       }


  HdfsPath should be able to list files/directories/paths at a single level

    List a single file

      ...

    'listFiles' is consistent with 'determineFile'

      ...

    List multiple files

      ...

    List a directory

      ...

    List multiple paths

      ...


  HdfsPath should be able to list files/directories/paths recursively

    List files

      ...

    List directories

      ...

    List paths

      ...

  HdfsPath should be able to glob files/directories/paths at a single level

    ...

  HdfsPath should be able to glob files/directories/paths recursively

    Glob files

      ...

    Glob directories

      ...

    Glob paths

      ...

- [ ] mkdirRetry
- [ ] list
- [ ] glob

"""
  val beFile = beSome(be_-\/[HdfsFile])
  val beDirectory = beSome(be_\/-[HdfsDirectory])

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
