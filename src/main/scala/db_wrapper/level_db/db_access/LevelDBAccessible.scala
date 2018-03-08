package db_wrapper.level_db.db_access

import db_wrapper.DBFilePath
import db_wrapper.level_db.LevelDBOptionsAccessible
import org.iq80.leveldb.DB
import org.iq80.leveldb.impl.Iq80DBFactory.factory

trait LevelDBAccessible extends DBAccessible with LevelDBOptionsAccessible with DBFilePath {
  protected lazy val db: DB = factory.open(dbFilePath.jfile, options)
}
