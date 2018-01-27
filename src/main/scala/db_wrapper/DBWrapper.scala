package db_wrapper

import java.io.Closeable
import java.security.MessageDigest

import db_wrapper.DBWrapper.{Deserializer, KeySerializer, ValueSerializer}

import scala.util.Try

trait DBWrapper extends Closeable {
  def write[K, V](key: K, value: V)(implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]): Try[Unit]
  def read[K, V](key: K)(implicit keySerializer: KeySerializer[K], valueDeserializer: Deserializer[V]): Try[V]
  def delete[K](key: K)(implicit keySerializer: KeySerializer[K]): Try[Unit]
}

object DBWrapper {

  private val digester = MessageDigest.getInstance("SHA-1")

  trait KeySerializer[T] {
    def getDigest(a: T): Array[Byte]
  }

  object KeySerializer {
    implicit object StringSerializer extends KeySerializer[String] {
      override def getDigest(a: String): Array[Byte] = {
        digester.digest(a.getBytes("UTF-8"))
      }
    }

    implicit object LongSerializer extends KeySerializer[Long] {
      override def getDigest(a: Long): Array[Byte] = {
        val ret = new Array[Byte](8)
        var i = 0
        while (i < 8) {
          ret(i) = (a >> (8 - i - 1 << 3)).toByte
          i += 1
        }
        digester.digest(ret)
      }
    }
  }

  trait ValueSerializer[T] {
    def toBytes(a: T): Array[Byte]
  }
  object ValueSerializer {
    implicit object StringSerializer extends ValueSerializer[String] {
      def toBytes(a: String): Array[Byte] = a.getBytes()
    }
  }

  trait Deserializer[T] {
    def fromBytes(bytes: Array[Byte]): T
  }
  object Deserializer {
    implicit object StringDeserializer extends Deserializer[String] {
      def fromBytes(bytes: Array[Byte]): String = bytes.toString
    }
  }
}
