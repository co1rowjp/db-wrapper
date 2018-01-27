package db_wrapper.level_db

import com.typesafe.scalalogging.LazyLogging
import db_wrapper.DBWrapper.KeySerializer
import org.iq80.leveldb.Options
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.reflect.io.Path

class LevelDBImplTest extends FlatSpec with BeforeAndAfter with LazyLogging {
  val dbFile = Path("./work/LevelDBImplTest.db")

  before {
    dbFile.deleteRecursively()
  }

  after {
    dbFile.deleteRecursively()
  }

  "db implementation" should "run" in {

    val dbWrapper = new LevelDBImpl() {
      val dbFilePath: Path = dbFile
      val options = new Options()
    }
    def deleteTest[K](iterable: Iterable[(K, String)])(implicit keySerializer: KeySerializer[K]) {
      dbWrapper.delete(iterable.head._1)
      dbWrapper.read(iterable.head._1).toEither.isLeft === true
      dbWrapper.read(iterable.tail.head._1) === iterable.tail.head._2
    }

    val stringKeyValues = Array(("keyA", "valueA"), ("kayB", "valueB"))
    val longKeyValues = Array((100L, "longValue100"), (200L, "longValue200"))
    try {
      stringKeyValues.foreach(kv => dbWrapper.write(kv._1, kv._2))
      longKeyValues.foreach(kv => dbWrapper.write(kv._1, kv._2))

      stringKeyValues.foreach(kv => dbWrapper.read(kv._1) === kv._2)
      longKeyValues.foreach(kv => dbWrapper.read(kv._1) === kv._2)

      deleteTest(longKeyValues)
      deleteTest(stringKeyValues)
    } finally {
      dbWrapper.close()
      dbFile.deleteRecursively()
    }
  }
}
