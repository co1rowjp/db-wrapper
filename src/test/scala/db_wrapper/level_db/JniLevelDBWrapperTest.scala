package db_wrapper.level_db

import db_wrapper.{DBWrapper, DBWrapperTest}
import org.iq80.leveldb.Options

import scala.reflect.io.Path

class JniLevelDBWrapperTest extends DBWrapperTest {
  val dbFile = Path("./work/LevelDBJniImplTest.db")
  val dbWrapper: DBWrapper = new JniLevelDBWrapper(dbFile, new Options)

  "db implementation" should "run" in {
    defaultTest(Array(("keyA", "valueA"), ("kayB", "valueB")))
    defaultTest(Array((100L, "longValue100"), (200L, "longValue200")))
    defaultTest(Array((100, "IntValue100"), (200, "IntValue200")))
  }
}
