package db_wrapper.level_db_jni

import java.io.Closeable

import db_wrapper.DBWrapper.{ValueDeserializer, ValueSerializer}
import db_wrapper.level_db.LevelDBOptionsAccessible
import db_wrapper.{DBFilePath, DBWrapper}
import org.fusesource.leveldbjni.JniDBFactory._

import scala.util.Try
import scalaz.syntax.id._

trait LevelDBJNIImpl extends DBWrapper with LevelDBOptionsAccessible with DBFilePath with Closeable {
  private lazy val db = factory.open(dbFilePath.jfile, options)

  def write[V](value: V)(implicit valueSerializer: ValueSerializer[V]): Try[Array[Byte]] = {
    val key = getDigest(value)
    Try {
      db.put(key, valueSerializer.toBytes(value))
      key
    }
  }

  def write[K, V](key: K, value: V)(implicit  keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]): Try[Unit] = {
    Try { db.put(getDigest(key), valueSerializer.toBytes(value)) }
  }

  def read[K, V](key: K)(implicit  keySerializer: ValueSerializer[K], valueDeserializer: ValueDeserializer[V]): Try[V] = {
    Try { getDigest(key) |> db.get |> valueDeserializer.fromBytes }
  }

  def delete[K](key: K)(implicit  keySerializer: ValueSerializer[K]): Try[Unit] = {
    Try { getDigest(key) |> db.delete }
  }

  def close(): Unit = {
    db.close()
  }
}