package db_wrapper

import java.io.Closeable
import java.security.MessageDigest

import db_wrapper.DBWrapper._

trait DBWrapper extends Closeable {
  private val digester = MessageDigest.getInstance("SHA-1")
  def getDigest[T](a: T)(implicit valueSerializer: ValueSerializer[T]): Array[Byte] = digester.digest(valueSerializer.toBytes(a))

  def write[K, V](key: K, value: V)(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]):  Either[Throwable, Unit]
  def read[K, V](key: K)(implicit keySerializer: ValueSerializer[K], valueDeserializer: ValueDeserializer[V]): Either[Throwable, V]
  def delete[K](key: K)(implicit keySerializer: ValueSerializer[K]): Either[Throwable, Unit]
}

object DBWrapper {

  trait ValueSerializer[T] {
    def toBytes(a: T): Array[Byte]
  }
  object ValueSerializer {
    implicit object StringSerializer extends ValueSerializer[String] {
      def toBytes(a: String): Array[Byte] = a.getBytes("UTF-8")
    }
    implicit object IntSerializer extends ValueSerializer[Int] {
      def toBytes(a: Int): Array[Byte] = {
        val ret = new Array[Byte](4)
        var i = 0
        while (i < 4) {
          ret(i) = (a >> (8 - i - 1 << 3)).toByte
          i += 1
        }
        ret
      }
    }
    implicit object LongSerializer extends ValueSerializer[Long] {
      def toBytes(a: Long): Array[Byte] = {
        val ret = new Array[Byte](8)
        var i = 0
        while (i < 8) {
          ret(i) = (a >> (8 - i - 1 << 3)).toByte
          i += 1
        }
        ret
      }
    }
  }

  trait ValueDeserializer[T] {
    def fromBytes(bytes: Array[Byte]): T
  }
  object ValueDeserializer {
    implicit object StringDeserializer extends ValueDeserializer[String] {
      def fromBytes(bytes: Array[Byte]): String = new String(bytes)
    }
  }
}
