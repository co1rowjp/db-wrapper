package db_wrapper

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.reflect.io.Path

trait SimpleDBWrapperTest  extends FlatSpec with BeforeAndAfterAll with LazyLogging {
  val dbFile: Path
  val dbWrapper: SimpleDBWrapper

  override def beforeAll() {
    dbFile.deleteRecursively()
  }

  override def afterAll() {
    dbWrapper.close()
    dbFile.deleteRecursively()
  }

  def deleteTest[K, V](iterable: Iterable[(K, V)]) {
    assert(dbWrapper.delete(iterable.head._1).isRight === true)
    assert(dbWrapper.read(iterable.head._1).isLeft === true)
    readTestImpl((iterable.tail.head._1, iterable.tail.head._2))
  }

  def writeTest[K, V](iterable: Iterable[(K, V)]) {
    iterable.foreach(kv =>
      dbWrapper.write(kv._1, kv._2) match {
        case Right(e) =>
        case Left(e) =>
          e.printStackTrace()
          fail()
      }
    )
  }

  def readTest[K, V](iterable: Iterable[(K, V)]) {
    iterable.foreach(kv => readTestImpl((kv._1, kv._2)))
  }
  def readTestImpl[K, V](kv: (K, V)) {
    dbWrapper.read[K,V](kv._1) match {
      case Right(v) =>
        assert(v === kv._2)
      case Left(e) =>
        e.printStackTrace()
        fail()
    }
  }

  def defaultTest[K, V](iterable: Iterable[(K, V)]) {
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
