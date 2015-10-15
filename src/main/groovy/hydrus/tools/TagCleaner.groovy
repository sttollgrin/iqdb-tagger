package hydrus.tools

import hydrus.db.DBProvider
import hydrus.db.hydrus.HydrusCache
import hydrus.db.providers.SqliteProvider
import org.apache.commons.lang.StringUtils
import org.sqlite.SQLiteErrorCode

import java.sql.SQLException
import java.util.logging.ConsoleHandler
import java.util.logging.Level
import java.util.logging.Logger

class TagCleaner {

    static {
        def logger = Logger.getLogger('groovy.sql')
        logger.level = Level.SEVERE
        logger.addHandler(new ConsoleHandler(level: Level.SEVERE))
    }

    public static void main(String[] args) {
        SqliteProvider db = new SqliteProvider([db_location: 'e:/Hydrus2/db/client.db'])
        HydrusCache.instance.init(db)
/*        fixNamespaces(db)
        mergeNamespaces(db, 'character', ['chracter', 'charracter', 'charcter'])
        mergeNamespaces(db, '', ['>', '3', 'd', '4'])
        mergeNamespaces(db, 'artist', ['creator', 'author'])
        mergeNamespaces(db, 'series', ['copyright', 'seires'])
*/

/*
        db.sql.eachRow('SELECT h.hash_id, h.hash, f.mime FROM files_info f JOIN hashes h ON f.hash_id = h.hash_id') { row ->
            String hash = row.hash.encodeHex().toString()
            def hashId = row.hash_id
            if (new File(hash + '.out').exists()) {
                deleteTagsForHash(db, hashId)
            }
        }
*/
//        deleteTag(db,HydrusCache.instance.tags.get('L'))
//        deleteNamespace(db,81)
        mergeTags(db, HydrusCache.instance.tags.get('fur'), HydrusCache.instance.tags.get('autotagged'), HydrusCache.instance.namespaces.get('sys'))

    }

    static void deleteTagsForHash(DBProvider db, Long hashId) {
        db.sql.execute('DELETE FROM mappings WHERE hash_id = ?', [hashId])
    }

    public static void deleteTag(SqliteProvider db, long tagid) {
        db.sql.execute('DELETE FROM mappings WHERE tag_id = ?', [tagid])
        db.sql.execute('DELETE FROM existing_tags WHERE tag_id = ?', [tagid])
        db.sql.execute('DELETE FROM tags WHERE tag_id = ?', [tagid])
    }

    // FIXME: use parenting instead of actually deleting tags
    public static void mergeTags(SqliteProvider db, long tagid1, long tagid2, long optionalNewNamespaceId = null) {
        if (optionalNewNamespaceId) {
            db.sql.executeUpdate('UPDATE OR IGNORE existing_tags SET namespace_id=? WHERE tag_id=?', [optionalNewNamespaceId, tagid1])
            db.sql.executeUpdate('UPDATE mappings SET tag_id=?, namespace_id=? WHERE tag_id=?', [tagid1, optionalNewNamespaceId, tagid2])
        } else {
            db.sql.executeUpdate('UPDATE mappings SET tag_id=? WHERE tag_id=?', [tagid1, tagid2])
        }
        deleteTag(db, tagid2)
    }

    public static void regenerateAutoCompletionCache() {
        //TODO
    }

    public static void copyTags(SqliteProvider db, String hash1, String hash2) {
        //TODO copy all tags between two images
    }

    public static void mergeNamespaces(SqliteProvider db, long namespaceid1, long namespaceid2) {
        db.sql.executeUpdate('UPDATE OR IGNORE existing_tags SET namespace_id=? WHERE namespace_id=?', [namespaceid1, namespaceid2])
        db.sql.executeUpdate('UPDATE OR IGNORE mappings SET namespace_id=? WHERE namespace_id=?', [namespaceid1, namespaceid2])
        deleteNamespace(db, namespaceid2)

    }

    public static void deleteNamespace(SqliteProvider db, long namespaceid) {
        db.sql.execute('DELETE FROM existing_tags WHERE namespace_id=?', [namespaceid])
        db.sql.execute('DELETE FROM mappings WHERE namespace_id=?', [namespaceid])
        db.sql.execute('DELETE FROM namespaces WHERE namespace_id=?', [namespaceid])
    }

    public static void deleteUnusedNamespaces(SqliteProvider db) {

    }

    public static void fixNamespaces(SqliteProvider db) {

        db.sql.eachRow('SELECT namespace_id, namespace FROM namespaces') { namespace ->
            if (StringUtils.isBlank(namespace.namespace)) {
                return
            }
            println "fixing ${namespace.namespace}"
            db.sql.withTransaction {
                db.sql.eachRow('SELECT t.tag, t.tag_id, e.namespace_id FROM tags t JOIN existing_tags e ON t.tag_id=e.tag_id WHERE e.namespace_id = ?', [namespace.namespace_id]) { tag ->
                    if (tag.tag.startsWith(namespace.namespace + ':')) {
                        String newTagName = tag.tag.substring(namespace.namespace.length() + 1)
                        try {
                            updateTag(db, tag.tag_id, newTagName)
                        } catch (SQLException ex) {
                            if (ex.errorCode == SQLiteErrorCode.SQLITE_CONSTRAINT.code) {
                                mergeTags(db, HydrusCache.instance.tags.get(newTagName), tag.tag_id, namespace.namespace_id)
                            } else {
                                throw ex
                            }
                        }
                    }
                }
            }
        }
    }

    public static void updateTag(SqliteProvider db, long tagid, String newTagName) {
        db.sql.executeUpdate("UPDATE tags SET tag = ${newTagName} WHERE tag_id = ${tagid}")
    }

    public static void mergeNamespaces(SqliteProvider db, String resultingNamespace, List sourceNamespaces) {
        long namespaceid = HydrusCache.instance.namespaces.get(resultingNamespace)
        db.sql.withTransaction {
            sourceNamespaces.each {
                long sourceid = HydrusCache.instance.namespaces.get(it)
                mergeNamespaces(db, namespaceid, sourceid)
            }
        }
    }

    public static void wipeDeletedMappings(SqliteProvider db) {
        db.sql.execute('DELETE FROM mappings WHERE status = 2')

    }


}
