package db_wrapper

import java.io._

trait SimpleDBWrapper extends Closeable {
  def write[K, V](key: K, value: V): Either[Throwable, Unit]

  def read[K, V](key: K): Either[Throwable, V]

  def delete[K](key: K): Either[Throwable, Unit]
}

object SimpleDBWrapper {

  object DefaultSerializer {
    def toBytes[T](a: T): Array[Byte] = {
      using(new ByteArrayOutputStream()) { (bout) =>
        using(new ObjectOutputStream(bout)) { (out) =>
          out.writeObject(a)
          out.flush()
          bout.toByteArray
        }
      }
    }
  }

  object DefaultDeserializer {
    def fromBytes[T](bytes: Array[Byte]): T = {
      using(new ObjectInputStream(new ByteArrayInputStream(bytes))) { (in) =>
        in.readObject().asInstanceOf[T]
      }
    }
  }

  def using[T <: Closeable, V](r: T)(f: (T) => V): V = {
    try {
      f(r)
    } finally {
      r.close()
    }
  }
}
