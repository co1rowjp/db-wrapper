package db_wrapper.level_db

import java.io.Closeable

import db_wrapper.DBWrapper.{ValueDeserializer, ValueSerializer}
import db_wrapper.SimpleDBWrapper
import db_wrapper.SimpleDBWrapper.{DefaultDeserializer, DefaultSerializer}
import db_wrapper.level_db.db_access.DBAccessible

import scala.util.Try
import scalaz.syntax.id._

trait SimpleLevelDBWrapper extends SimpleDBWrapper with DBAccessible with Closeable {

  def write[K, V](key: K, value: V):  Either[Throwable, Unit] = {
    Try { db.put(DefaultSerializer.toBytes(key), DefaultSerializer.toBytes(value)) }.toEither
  }

  def read[K, V](key: K)(implicit keySerializer: ValueSerializer[K], valueDeserializer: ValueDeserializer[V]):  Either[Throwable, V] = {
    Try { DefaultSerializer.toBytes(key) |> db.get |> DefaultDeserializer.fromBytes }.toEither
  }

  def delete[K](key: K)(implicit keySerializer: ValueSerializer[K]):  Either[Throwable, Unit] = {
    Try { DefaultSerializer.toBytes(key) |> db.delete }.toEither
  }

  def close(): Unit = {
    db.close()
  }
}