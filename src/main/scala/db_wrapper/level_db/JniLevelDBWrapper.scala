package db_wrapper.level_db

import scala.reflect.io.Path

import db_wrapper.level_db.db_access.JniLevelDBAccessible
import db_wrapper.level_db.impl.LevelDBWrapperImpl
import org.iq80.leveldb.Options

class JniLevelDBWrapper(protected val dbFilePath: Path, protected val options: Options) extends LevelDBWrapperImpl with JniLevelDBAccessible
