package hydrus.db.hydrus

import com.google.common.cache.LoadingCache
import hydrus.db.AbstractCache
import hydrus.db.DBProvider

@Singleton
class HydrusCache extends AbstractCache {

    LoadingCache<String, Object> tags
    LoadingCache<String, Object> namespaces

    void init(DBProvider db) {
        tags = addRegion('tags', db, 'SELECT tag_id as id FROM tags WHERE tag = ?', 'INSERT INTO tags (tag) VALUES (?)', true)
        namespaces = addRegion('namespaces', db,
                'SELECT namespace_id as id FROM namespaces WHERE namespace = ?',
                'INSERT INTO namespaces (namespace) VALUES (?)',
                'SELECT namespace, namespace_id FROM namespaces', true)
    }

}

