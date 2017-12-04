package db_wrapper.level_db

import com.typesafe.scalalogging.LazyLogging
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
      val dbFilePath = dbFile
      val options = new Options()
    }
    val stringKeyValues = Array(("keyA", "valueA"), ("kayB", "valueB"))
    val longKeyValues = Array((100L, "longValue100"), (200L, "longValue200"))
    try {
      stringKeyValues.foreach(kv => dbWrapper.write(kv._1, kv._2))
      longKeyValues.foreach(kv => dbWrapper.write(kv._1, kv._2))

      stringKeyValues.foreach(kv => dbWrapper.read(kv._1) === kv._2)
      longKeyValues.foreach(kv => dbWrapper.read(kv._1) === kv._2)
    } finally {
      dbWrapper.close()
      dbFile.deleteRecursively()
    }
  }
}
