package db_wrapper.level_db

import org.iq80.leveldb.Options

trait LevelDBOptionsAccessible {
  val options: Options
}
