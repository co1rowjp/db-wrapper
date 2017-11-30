package db_wrapper

import java.security.MessageDigest

import scala.util.Try

trait DBWrapper {
  def write[K, V](key: K, value: V)(implicit keySerializer: DBWrapper.KeySerializer[K], valueSerializer: DBWrapper.ValueSerializer[V]): Try[Unit]
  def read[K, V](key: K)(implicit keySerializer: DBWrapper.KeySerializer[K], valueDeserializer: DBWrapper.Deserializer[V]): Try[V]
  def shutdown()
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
  }

  trait ValueSerializer[T] {
    def toBytes(a: T): Array[Byte]
  }
  object ValueSerializer {
    implicit object StringSerializer extends ValueSerializer[String] {
      def toBytes(a: String) = a.getBytes()
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
