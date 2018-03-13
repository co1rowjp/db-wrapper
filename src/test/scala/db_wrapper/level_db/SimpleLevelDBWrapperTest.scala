package db_wrapper.level_db

import db_wrapper.{SimpleDBWrapper, SimpleDBWrapperTest}
import org.iq80.leveldb.Options

import scala.reflect.io.Path

class SimpleLevelDBWrapperTest extends SimpleDBWrapperTest {
  val dbFile = Path("./work/LevelDBImplTest.db")
  val dbWrapper: SimpleDBWrapper = new SimpleLevelDBWrapper(dbFile, new Options())

  "db implementation" should "run" in {
    defaultTest(Array(("keyA", "valueA"), ("kayB", "valueB")))
    defaultTest(Array((100L, "longValue100"), (200L, "longValue200")))
    defaultTest(Array((1001, "IntValue100"), (2001, "IntValue200")))
  }

}
