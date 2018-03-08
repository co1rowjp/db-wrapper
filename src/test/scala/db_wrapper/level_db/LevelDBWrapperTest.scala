package db_wrapper.level_db

import java.nio.ByteBuffer

import db_wrapper.DBWrapper.{ValueDeserializer, ValueSerializer}
import db_wrapper.level_db.db_access.LevelDBAccessible
import db_wrapper.{DBWrapper, DBWrapperTest}
import org.iq80.leveldb.Options

import scala.reflect.io.Path

class LevelDBWrapperTest extends DBWrapperTest {
  val dbFile = Path("./work/LevelDBImplTest.db")
  val dbWrapper: DBWrapper = new LevelDBWrapper() with LevelDBAccessible {
    val dbFilePath: Path = dbFile
    val options = new Options()
  }

  "db implementation" should "run" in {
    defaultTest(Array(("keyA", "valueA"), ("kayB", "valueB")))
    defaultTest(Array((100L, "longValue100"), (200L, "longValue200")))
    defaultTest(Array((1001, "IntValue100"), (2001, "IntValue200")))
  }

  "inject deserializer" should "run" in {
    case class TestMock(i: Int, s: String)

    implicit object TestMockSerializer extends ValueSerializer[TestMock] {
      def toBytes(a: TestMock): Array[Byte] = {
        val strBuf = a.s.getBytes("UTF-8")
        val buf = ByteBuffer.allocate(4 + 4 + 4 + strBuf.length)
        buf.putInt(1) // serialize version
        buf.putInt(a.i)
        buf.putInt(strBuf.length)
        buf.put(strBuf)
        buf.array()
      }
    }
    implicit object TestMockDeserializer extends ValueDeserializer[TestMock] {
      def fromBytes(bytes: Array[Byte]): TestMock = {
        val buf = ByteBuffer.wrap(bytes)
        val deserializeVersion = buf.getInt()
        deserializeVersion match {
          case 1 =>
            val id = buf.getInt()
            val len = buf.getInt()
            val str = new Array[Byte](len)
            buf.get(str)
            TestMock(id, new String(str))
          case _ =>
            throw new IllegalArgumentException(s"class[TestMock] unsupported deserializeVersion: $deserializeVersion")
        }
      }
    }
    defaultTest(Array((1L, TestMock(1, "hoge")), (2L, TestMock(2, "fuga"))))
  }
}
