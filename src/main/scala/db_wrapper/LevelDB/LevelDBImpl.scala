package db_wrapper.LevelDB

import java.io.File
import java.nio.file.Path

import db_wrapper.DBWrapper
import org.iq80.leveldb.Options
import scalaz.syntax.id._

import scala.util.Try

class LevelDBImpl(dbPath: Path) extends DBWrapper {
  private val options = new Options()
  private val db = org.iq80.leveldb.impl.Iq80DBFactory.factory.open(new File(dbPath.toAbsolutePath.toString), options)

  def write[K, V](key: K, value: V)(implicit keySerializer: DBWrapper.KeySerializer[K], valueSerializer: DBWrapper.ValueSerializer[V]): Try[Unit] = {
    Try { db.put(keySerializer.getDigest(key), valueSerializer.toBytes(value)) }
  }

  def read[K, V](key: K)(implicit keySerializer: DBWrapper.KeySerializer[K], valueDeserializer: DBWrapper.Deserializer[V]): Try[V] = {
    Try { keySerializer.getDigest(key) |> db.get |> valueDeserializer.fromBytes }
  }

  def shutdown(): Unit = {
    db.close()
  }
}
