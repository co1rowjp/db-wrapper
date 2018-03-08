package db_wrapper.level_db.db_access

import db_wrapper.DBFilePath
import db_wrapper.level_db.LevelDBOptionsAccessible
import org.iq80.leveldb.DB

trait JniLevelDBAccessible  extends DBAccessible with LevelDBOptionsAccessible with DBFilePath {
  protected lazy val db: DB =  org.fusesource.leveldbjni.JniDBFactory.factory.open(dbFilePath.jfile, options)
}
