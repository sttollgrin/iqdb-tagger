package hydrus.tools

import groovy.json.JsonSlurper
import hydrus.db.DBProvider
import hydrus.db.hydrus.HydrusCache
import hydrus.db.providers.SqliteProvider

/*
 * imports hydrus json files waaay faster than original tool - with this it took 6 hours, with original tool I gave up after 4 days
 * maybe because I'm skipping a lot of processing here...
 */

class JsonMapper {

    public static void main(String[] args) {
        SqliteProvider db = new SqliteProvider([db_location: 'e:/Hydrus2/db/client.db'])
        HydrusCache.instance.init(db)

        new JsonMapper(db).startMapping('e:/cu/', '1434270343_2.json')
    }

    private final DBProvider db

    public JsonMapper(DBProvider db) {
        this.db = db
    }

    public void startMapping(String jsonDir, String firstFile) {
        boolean skip = true
        new File(jsonDir).eachFile { file ->
            if (file.name.matches(/\d+_\d+\.json/)) {
                if (file.name.equals(firstFile)) {
                    skip = false
                }
                if (skip) {
                    return
                }
                def start = System.currentTimeMillis()
                int mappingCount = 0
                int tagCount = 0
                int hashCount = 0
                def jsonSlurper = new JsonSlurper()
                def object = jsonSlurper.parseText(file.text)
                def fileTags = object[2][0][0][1][0][1]
                def fileMappings = object[2][1]

                db.sql.withTransaction {
                    def mappingCache = new HashMap<Long, Long>()
                    fileMappings.each {
                        ++hashCount
                        Long tempId = it[0]
                        Long dbId = createHash(it[1])
                        mappingCache.put(tempId, dbId)
                    }

                    fileTags.each { fileTag ->
                        ++tagCount
                        String tag = fileTag[0]
                        def mappings = fileTag[1]
                        mappings.each {
                            ++mappingCount
                            def hashId = mappingCache.get(it.longValue())
                            String namespace = tag.contains(':') ? tag.split(':')[0] : ''
                            HydrusIQDBTagger.createMapping(db, hashId, HydrusCache.instance.tags.get(tag), HydrusCache.instance.namespaces.get(namespace))
                        }
                    }
                }
                def end = System.currentTimeMillis()
                println "processed ${file.name}, t: ${tagCount}, h: ${hashCount}, m: ${mappingCount}, time: ${(end - start) / 1000.0}s, rate: ${mappingCount / ((end - start) / 1000.0)}I/s"
            }
        }
    }

    long createHash(String hash) {
        db.sql.executeInsert("INSERT OR IGNORE INTO hashes (hash) VALUES (${hash.decodeHex()})")[0][0]
    }

}
