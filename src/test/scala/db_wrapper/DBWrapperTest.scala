package db_wrapper

import com.typesafe.scalalogging.LazyLogging
import db_wrapper.DBWrapper.{KeySerializer, ValueSerializer}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.reflect.io.Path

trait DBWrapperTest extends FlatSpec with BeforeAndAfter with LazyLogging {
  val dbFile: Path
  val dbWrapper: DBWrapper

  before {
    dbFile.deleteRecursively()
  }

  after {
    dbWrapper.close()
    dbFile.deleteRecursively()
  }

  def deleteTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]) {
    dbWrapper.delete(iterable.head._1)
    dbWrapper.read(iterable.head._1).toEither.isLeft === true
    dbWrapper.read(iterable.tail.head._1) === iterable.tail.head._2
  }

  def writeTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]) {
    iterable.foreach(kv => dbWrapper.write(kv._1, kv._2))
  }

  def readTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]) {
    iterable.foreach(kv => dbWrapper.read(kv._1) === kv._2)
  }

  def defaultTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]) {
    writeTest(iterable)
    readTest(iterable)
    deleteTest(iterable)
  }

  def benchmark(max: Int) {
    logger.info(s"<DBWrapperTest> benchmark start. writing $max data")
    for (i <- 0 until max) {
      dbWrapper.write(i, i.toString)
    }
    logger.info(s"<DBWrapperTest> benchmark finish.")
  }
}
