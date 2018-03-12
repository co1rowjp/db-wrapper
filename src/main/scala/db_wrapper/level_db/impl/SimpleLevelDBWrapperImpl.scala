package db_wrapper.level_db.impl

import java.io.Closeable

import db_wrapper.DBWrapper.{ValueDeserializer, ValueSerializer}
import db_wrapper.SimpleDBWrapper
import db_wrapper.SimpleDBWrapper.{DefaultDeserializer, DefaultSerializer}
import db_wrapper.level_db.db_access.DBAccessible

import scala.util.Try
import scalaz.syntax.id._

trait SimpleLevelDBWrapperImpl extends SimpleDBWrapper with DBAccessible with Closeable {

  def write[K, V](key: K, value: V):  Either[Throwable, Unit] = {
    Try { db.put(DefaultSerializer.toBytes(key), DefaultSerializer.toBytes(value)) }.toEither
  }

  def read[K, V](key: K): Either[Throwable, V] = {
    Try { DefaultSerializer.toBytes(key) |> db.get |> DefaultDeserializer.fromBytes }.toEither
  }

  def delete[K](key: K): Either[Throwable, Unit] = {
    Try { DefaultSerializer.toBytes(key) |> db.delete }.toEither
  }

  def close(): Unit = {
    db.close()
  }
}
