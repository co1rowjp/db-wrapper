package db_wrapper.level_db

import java.io.Closeable

import db_wrapper.DBWrapper.ValueDeserializer.DefaultDeserializer
import db_wrapper.DBWrapper.ValueSerializer.DefaultSerializer
import db_wrapper.{DBFilePath, DBWrapper}
import db_wrapper.DBWrapper._
import org.iq80.leveldb.impl.Iq80DBFactory._

import scalaz.syntax.id._
import scala.util.Try

trait LevelDBImpl extends DBWrapper with LevelDBOptionsAccessible with DBFilePath with Closeable {
  private lazy val db = factory.open(dbFilePath.jfile, options)

  def write[V](value: V)(implicit valueSerializer: ValueSerializer[V]): Either[Throwable, Array[Byte]] = {
    val key = getDigest(value)
    Try {
      db.put(key, valueSerializer.toBytes(value))
      key
    }.toEither
  }

  def write[K, V](key: K, value: V)(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]):  Either[Throwable, Unit] = {
    Try { db.put(getDigest(key), valueSerializer.toBytes(value)) }.toEither
  }

  def read[K, V](key: K)(implicit keySerializer: ValueSerializer[K], valueDeserializer: ValueDeserializer[V]):  Either[Throwable, V] = {
    Try { getDigest(key) |> db.get |> valueDeserializer.fromBytes }.toEither
  }

  def delete[K](key: K)(implicit keySerializer: ValueSerializer[K]):  Either[Throwable, Unit] = {
    Try { getDigest(key) |> db.delete }.toEither
  }

  def close(): Unit = {
    db.close()
  }

  def writeSimple[K, V](key: K, value: V)(implicit keySerializer: ValueSerializer[K]): Either[Throwable, Unit] = {
    Try { db.put(getDigest(key), DefaultSerializer.toBytes(value)) }.toEither
  }

  def readSimple[K, V](key: K)(implicit keySerializer: ValueSerializer[K]): Either[Throwable, V] = {
    Try { getDigest(key) |> db.get |> DefaultDeserializer.fromBytes }.toEither
  }
}
