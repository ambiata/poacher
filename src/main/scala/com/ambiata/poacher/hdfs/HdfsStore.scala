package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import com.ambiata.mundane.store._
import java.util.UUID
import java.io.{InputStream, OutputStream}
import java.io.{PipedInputStream, PipedOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO, effect.Effect._, \&/._
import scodec.bits.ByteVector
import FilePath._

case class HdfsStore(conf: Configuration, base: DirPath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  def readOnly: ReadOnlyStore[ResultTIO] =
    this

  def basePath: Path =
    new Path(base.path)

  def list(prefix: DirPath): ResultT[IO, List[FilePath]] =
    hdfs { Hdfs.filesystem.flatMap { fs =>
      Hdfs.globFilesRecursively(base </> prefix).map { paths =>
        paths.map(path => FilePath.unsafe(path.toString).relativeTo(base </> prefix))
      }
    }}

  def filter(prefix: DirPath, predicate: FilePath => Boolean): ResultT[IO, List[FilePath]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: DirPath, predicate: FilePath => Boolean): ResultT[IO, Option[FilePath]] =
    list(prefix).map(_.find(predicate))

  def exists(path: FilePath): ResultT[IO, Boolean] =
    hdfs { Hdfs.exists(base </> path) }

  def exists(path: DirPath): ResultT[IO, Boolean] =
    hdfs { Hdfs.exists(base </> path) }

  def delete(path: FilePath): ResultT[IO, Unit] =
    hdfs { Hdfs.delete(base </> path) }

  def deleteAll(prefix: DirPath): ResultT[IO, Unit] =
    hdfs { Hdfs.deleteAll(base </> prefix) }

  def move(in: FilePath, out: FilePath): ResultT[IO, Unit] =
    copy(in, out) >> delete(in)

  def copy(in: FilePath, out: FilePath): ResultT[IO, Unit] =
    hdfs { Hdfs.cp(base </> in, base </> out, false) }

  def mirror(in: DirPath, out: DirPath): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU(source => copy(source, out </> source))
  } yield ()

  def moveTo(store: Store[ResultTIO], src: FilePath, dest: FilePath): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[ResultTIO], src: FilePath, dest: FilePath): ResultT[IO, Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[ResultTIO], in: DirPath, out: DirPath): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU(source => copyTo(store, source, out </> source))
  } yield ()

  def checksum(path: FilePath, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    withInputStreamValue[Checksum](path)(in => Checksum.stream(in, algorithm))

  val bytes: StoreBytes[ResultTIO] = new StoreBytes[ResultTIO] {
    def read(path: FilePath): ResultT[IO, ByteVector] =
      withInputStreamValue[Array[Byte]](path)(Streams.readBytes(_, 4 * 1024 * 1024)).map(ByteVector.view)

    def write(path: FilePath, data: ByteVector): ResultT[IO, Unit] =
      unsafe.withOutputStream(path)(Streams.writeBytes(_, data.toArray))

    def source(path: FilePath): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(FileSystem.get(conf).open(base </> path)).evalMap(_(1024 * 1024))

    def sink(path: FilePath): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(path: FilePath, codec: Codec): ResultT[IO, String] =
      bytes.read(path).map(b => new String(b.toArray, codec.name))

    def write(path: FilePath, data: String, codec: Codec): ResultT[IO, Unit] =
      bytes.write(path, ByteVector.view(data.getBytes(codec.name)))
  }

  val utf8: StoreUtf8[ResultTIO] = new StoreUtf8[ResultTIO] {
    def read(path: FilePath): ResultT[IO, String] =
      strings.read(path, Codec.UTF8)

    def write(path: FilePath, data: String): ResultT[IO, Unit] =
      strings.write(path, data, Codec.UTF8)

    def source(path: FilePath): Process[Task, String] =
      bytes.source(path) |> scalaz.stream.text.utf8Decode

    def sink(path: FilePath): Sink[Task, String] =
      bytes.sink(path).map(_.contramap(s => ByteVector.view(s.getBytes("UTF-8"))))
  }

  val lines: StoreLines[ResultTIO] = new StoreLines[ResultTIO] {
    def read(path: FilePath, codec: Codec): ResultT[IO, List[String]] =
      strings.read(path, codec).map(_.lines.toList)

    def write(path: FilePath, data: List[String], codec: Codec): ResultT[IO, Unit] =
      strings.write(path, Lists.prepareForFile(data), codec)

    def source(path: FilePath, codec: Codec): Process[Task, String] =
      scalaz.stream.io.linesR(FileSystem.get(conf).open(base </> path))(codec)

    def sink(path: FilePath, codec: Codec): Sink[Task, String] =
      bytes.sink(path).map(_.contramap(s => ByteVector.view(s"$s\n".getBytes(codec.name))))
  }

  val linesUtf8: StoreLinesUtf8[ResultTIO] = new StoreLinesUtf8[ResultTIO] {
    def read(path: FilePath): ResultT[IO, List[String]] =
      lines.read(path, Codec.UTF8)

    def write(path: FilePath, data: List[String]): ResultT[IO, Unit] =
      lines.write(path, data, Codec.UTF8)

    def source(path: FilePath): Process[Task, String] =
      lines.source(path, Codec.UTF8)

    def sink(path: FilePath): Sink[Task, String] =
      lines.sink(path, Codec.UTF8)
  }

  def withInputStreamValue[A](path: FilePath)(f: InputStream => ResultT[IO, A]): ResultT[IO, A] =
    hdfs { Hdfs.readWith(base </> path, f) }

  val unsafe: StoreUnsafe[ResultTIO] = new StoreUnsafe[ResultTIO] {
    def withInputStream(path: FilePath)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      withInputStreamValue[Unit](path)(f)

    def withOutputStream(path: FilePath)(f: OutputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      hdfs { Hdfs.writeWith(base </> path, f) }
  }

  def hdfs[A](thunk: => Hdfs[A]): ResultT[IO, A] =
    thunk.run(conf)

  private implicit def filePathToPath(f: FilePath): Path = new Path(f.path)
  private implicit def dirPathToPath(d: DirPath): Path = new Path(d.path)

}
