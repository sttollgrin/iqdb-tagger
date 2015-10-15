package hydrus.db.providers

import groovy.sql.Sql
import hydrus.db.DBProvider

class SqliteProvider extends DBProvider {

    public SqliteProvider(Map properties) {
        sql = Sql.newInstance("jdbc:sqlite:${properties.db_location}", "org.sqlite.JDBC")
    }

}
