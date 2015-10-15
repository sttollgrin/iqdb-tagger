package hydrus.db

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache

abstract class AbstractCache {

    def Map<String, LoadingCache<String, Object>> cacheRegions = new HashMap<>()

    public LoadingCache<String, Object> addRegion(String regionName, Closure valueGetClosure, boolean ignoreIfExists) {
        def region = cacheRegions[regionName]
        if (region) {
            if (!ignoreIfExists) {
                throw new IllegalStateException("Region '${regionName}' already exists in cache")
            }
            return region
        }
        cacheRegions[regionName] = CacheBuilder.newBuilder().build([load: valueGetClosure] as CacheLoader)
    }

    public LoadingCache<String, Object> addRegion(String regionName, DBProvider db, String selectSql, String insertSql, boolean ignoreIfExists) {
        addRegion(regionName, { String key ->
            def obj = db.sql.firstRow(selectSql, [key])
            if (obj != null) {
                return obj[0]
            } else {
                db.sql.executeInsert(insertSql, [key])[0][0]
            }
        }, ignoreIfExists)
    }

    public LoadingCache<String, Object> addRegion(String regionName, DBProvider db, String selectSql, String insertSql, String initSql, boolean ignoreIfExists) {
        def region = addRegion(regionName, { String key ->
            def obj = db.sql.firstRow(selectSql, [key])
            if (obj != null) {
                return obj[0]
            } else {
                db.sql.executeInsert(insertSql, [key])[0][0]
            }
        }, ignoreIfExists)

        region.putAll(db.sql.rows(initSql).collectEntries { [it[0], it[1]] })
        region
    }

    public LoadingCache<String, Object> getRegion(String regionName) {
        def region = cacheRegions[regionName]
        if (!region) {
            throw new IllegalStateException("Region ${regionName} does not exist in cache")
        }
        region
    }

    public Object get(String region, String code) {
        getRegion(region).get(code)
    }

    public void evict(String region, String code) {
        getRegion(region).invalidate(code)
    }

}
