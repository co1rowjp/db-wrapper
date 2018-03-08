package db_wrapper.level_db.db_access

trait DBAccessible {
  protected  val db: org.iq80.leveldb.DB
}
