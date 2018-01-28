package db_wrapper.level_db_jni

import java.io.Closeable

import db_wrapper.level_db.LevelDBOptionsAccessible
import db_wrapper.{DBFilePath, DBWrapper}
import org.fusesource.leveldbjni.JniDBFactory._

import scala.util.Try
import scalaz.syntax.id._

trait LevelDBJNIImpl extends DBWrapper with LevelDBOptionsAccessible with DBFilePath with Closeable {
  private lazy val db = factory.open(dbFilePath.jfile, options)

  def write[K, V](key: K, value: V)(implicit keySerializer: DBWrapper.KeySerializer[K], valueSerializer: DBWrapper.ValueSerializer[V]): Try[Unit] = {
    Try { db.put(keySerializer.getDigest(key), valueSerializer.toBytes(value)) }
  }

  def read[K, V](key: K)(implicit keySerializer: DBWrapper.KeySerializer[K], valueDeserializer: DBWrapper.Deserializer[V]): Try[V] = {
    Try { keySerializer.getDigest(key) |> db.get |> valueDeserializer.fromBytes }
  }

  def delete[K](key: K)(implicit keySerializer: DBWrapper.KeySerializer[K]): Try[Unit] = {
    Try {keySerializer.getDigest(key) |> db.delete }
  }

  def close(): Unit = {
    db.close()
  }
}