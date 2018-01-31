package db_wrapper

import com.typesafe.scalalogging.LazyLogging
import db_wrapper.DBWrapper._
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

  def deleteTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]) {
    dbWrapper.delete(iterable.head._1)
    assert(dbWrapper.read(iterable.head._1).isLeft === true)
    readTestImpl((iterable.tail.head._1, iterable.tail.head._2))
  }

  def writeTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]) {
    iterable.foreach(kv => dbWrapper.write(kv._1, kv._2))
  }

  def readTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]) {
    iterable.foreach(kv => readTestImpl((kv._1, kv._2)))
  }
  def readTestImpl[K, V](kv: (K, V))(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]) {
    dbWrapper.read(kv._1) match {
      case Right(v) => assert(v === kv._2)
      case Left(e) => assert(e === kv._2)
    }
  }

  def defaultTest[K, V](iterable: Iterable[(K, V)])(implicit keySerializer: ValueSerializer[K], valueSerializer: ValueSerializer[V]) {
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
