package com.ambiata.poacher.hdfs

import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, \&/._, effect.IO
import scodec.bits.ByteVector
import org.specs2._, org.specs2.matcher._
import org.scalacheck.Arbitrary, Arbitrary._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._, ResultTIOMatcher._
import java.io._
import java.util.UUID


// FIX Workout how this test can be pulled out and shared with posix/s3/hdfs.
class HdfsStoreSpec extends Specification with ScalaCheck { def is = args.execute(threadsNb = 10) ^ s2"""
  Hdfs Store Usage
  ================

  list path                                       $list
  list all files paths from a sub path            $listSubPath
  filter listed paths                             $filter
  find path in root (thirdish)                    $find
  find path in root (first)                       $findfirst
  find path in root (last)                        $findlast

  exists                                          $exists
  not exists                                      $notExists

  delete                                          $delete
  deleteAll                                       $deleteAll

  move                                            $move
  move and read                                   $moveRead
  copy                                            $copy
  copy and read                                   $copyRead
  mirror                                          $mirror

  moveTo                                          $moveTo
  copyTo                                          $copyTo
  mirrorTo                                        $mirrorTo

  checksum                                        $checksum

  read / write bytes                              $bytes
  read / write strings                            $strings
  read / write utf8 strings                       $utf8Strings
  read / write lines                              $lines
  read / write utf8 lines                         $utf8Lines

  """

  implicit val params =
    Parameters(workers = 20, minTestsOk = 40, maxSize = 10)

  val conf = new Configuration

  implicit def HdfsStoreArbitary: Arbitrary[HdfsStore] =
    Arbitrary(arbitrary[Int].map(math.abs).map(n =>
      HdfsStore(conf, DirPath.Root </> "tmp" </> FileName.unsafe(s"HdfsStoreSpec.${UUID.randomUUID}.${n}"))))

  def list =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
       store.list(DirPath.Empty) must beOkLike((_:List[FilePath]) must contain(exactly(filepaths:_*))) })

  def listSubPath =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths.map(_ prepend "sub")) { filepaths =>
      store.list(DirPath.Empty </> "sub") must beOkLike((_:List[FilePath]).toSet must_== filepaths.map(_.fromRoot).toSet) })

  def filter =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      val first = filepaths.head
      val last = filepaths.last
      val expected = if (first == last) List(first) else List(first, last)
      store.filter(DirPath.Empty, x => x == first || x == last) must beOkLike(paths => paths must contain(allOf(expected:_*))) })

  def find =
    prop((store: HdfsStore, paths: Paths) => paths.entries.length >= 3 ==> { clean(store, paths) { filepaths =>
      val third = filepaths.drop(2).head
      store.find(DirPath.Empty, _ == third) must beOkValue(Some(third)) } })

  def findfirst =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.find(DirPath.Empty, x => x == filepaths.head) must beOkValue(Some(filepaths.head)) })

  def findlast =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.find(DirPath.Empty, x => x == filepaths.last) must beOkValue(Some(filepaths.last)) })

  def exists =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      filepaths.traverseU(store.exists) must beOkLike(_.forall(identity)) })

  def notExists =
    prop((store: HdfsStore, paths: Paths) => store.exists(DirPath.Root </> "i really don't exist") must beOkValue(false))

  def delete =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      val first = filepaths.head
      (store.delete(first) >> filepaths.traverseU(store.exists)) must beOkLike(x => !x.head && x.tail.forall(identity)) })

  def deleteAll =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      (store.deleteAll(DirPath.Empty) >> filepaths.traverseU(store.exists)) must beOkLike(x => !x.tail.exists(identity)) })

  def move =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.move(FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.exists(FilePath.unsafe(m.full)).zip(store.exists(FilePath.unsafe(n.full)))) must beOkValue(false -> true) })

  def moveRead =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.move(FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.utf8.read(FilePath.unsafe(n.full))) must beOkValue(m.value.toString) })

  def copy =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.copy(FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.exists(FilePath.unsafe(m.full)).zip(store.exists(FilePath.unsafe(n.full)))) must beOkValue(true -> true) })

  def copyRead =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.copy(FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.utf8.read(FilePath.unsafe(m.full)).zip(store.utf8.read(FilePath.unsafe(n.full)))) must beOkLike({ case (in, out) => in must_== out }) })

  def mirror =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.mirror(DirPath.Empty, DirPath.unsafe("mirror")) >> store.list(DirPath.unsafe("mirror")) must
        beOkLike((_:List[FilePath]) must contain(exactly(filepaths.map("mirror" </> _):_*))) })

  def moveTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: Entry, n: Entry) => clean(store, alternate, Paths(m :: Nil)) { _ =>
      (store.moveTo(alternate, FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.exists(FilePath.unsafe(m.full)).zip(alternate.exists(FilePath.unsafe(n.full)))) must beOkValue(false -> true) })

  def copyTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: Entry, n: Entry) => clean(store, alternate, Paths(m :: Nil)) { _ =>
      (store.copyTo(alternate, FilePath.unsafe(m.full), FilePath.unsafe(n.full)) >>
       store.exists(FilePath.unsafe(m.full)).zip(alternate.exists(FilePath.unsafe(n.full)))) must beOkValue(true -> true) })

  def mirrorTo =
    prop((store: HdfsStore, alternate: HdfsStore, paths: Paths) => clean(store, alternate, paths) { filepaths =>
      store.mirrorTo(alternate, DirPath.Empty, DirPath.unsafe("mirror")) >> alternate.list(DirPath.unsafe("mirror")) must
        beOkLike((_:List[FilePath]) must contain(exactly(filepaths.map("mirror" </> _):_*))) })

  def checksum =
    prop((store: HdfsStore, m: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      store.checksum(FilePath.unsafe(m.full), MD5) must beOkValue(Checksum.string(m.value.toString, MD5)) })

  def bytes =
    prop((store: HdfsStore, m: Entry, bytes: Array[Byte]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.bytes.write(FilePath.unsafe(m.full), ByteVector(bytes)) >> store.bytes.read(FilePath.unsafe(m.full))) must beOkValue(ByteVector(bytes)) })

  def strings =
    prop((store: HdfsStore, m: Entry, s: String) => clean(store, Paths(m :: Nil)) { _ =>
      (store.strings.write(FilePath.unsafe(m.full), s, Codec.UTF8) >> store.strings.read(FilePath.unsafe(m.full), Codec.UTF8)) must beOkValue(s) })

  def utf8Strings =
    prop((store: HdfsStore, m: Entry, s: String) => clean(store, Paths(m :: Nil)) { _ =>
      (store.utf8.write(FilePath.unsafe(m.full), s) >> store.utf8.read(FilePath.unsafe(m.full))) must beOkValue(s) })

  def lines =
    prop((store: HdfsStore, m: Entry, s: List[Int]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.lines.write(FilePath.unsafe(m.full), s.map(_.toString), Codec.UTF8) >> store.lines.read(FilePath.unsafe(m.full), Codec.UTF8)) must beOkValue(s.map(_.toString)) })

  def utf8Lines =
    prop((store: HdfsStore, m: Entry, s: List[Int]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.linesUtf8.write(FilePath.unsafe(m.full), s.map(_.toString)) >> store.linesUtf8.read(FilePath.unsafe(m.full))) must beOkValue(s.map(_.toString)) })

  def files(paths: Paths): List[FilePath] =
    paths.entries.map(e => FilePath.unsafe(e.full).asRelative).sortBy(_.path)

  def create(store: HdfsStore, paths: Paths): ResultT[IO, Unit] =
    paths.entries.traverseU(e =>
      Hdfs.writeWith[Unit](store.base </> FilePath.unsafe(e.full), out => ResultT.safe[IO, Unit] { out.write( e.value.toString.getBytes("UTF-8")) }).run(conf)).void

  def clean[A](store: HdfsStore, paths: Paths)(run: List[FilePath] => A): A = {
    create(store, paths).run.unsafePerformIO
    try run(files(paths))
    finally store.deleteAll(DirPath.Empty).run.unsafePerformIO
  }

  def clean[A](store: HdfsStore, alternate: HdfsStore, paths: Paths)(run: List[FilePath] => A): A = {
    create(store, paths).run.unsafePerformIO
    try run(files(paths))
    finally (store.deleteAll(DirPath.Empty) >> alternate.deleteAll(DirPath.Empty)).run.unsafePerformIO
  }

  private implicit def filePathToPath(f: FilePath): Path = new Path(f.path)
  private implicit def dirPathToPath(d: DirPath): Path = new Path(d.path)

}
