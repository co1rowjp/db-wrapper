package db_wrapper.level_db

import db_wrapper.level_db.db_access.LevelDBAccessible
import db_wrapper.level_db.impl.SimpleLevelDBWrapperImpl
import org.iq80.leveldb.Options

import scala.reflect.io.Path

class SimpleLevelDBWrapper(protected val dbFilePath: Path, protected val options: Options) extends SimpleLevelDBWrapperImpl with LevelDBAccessible