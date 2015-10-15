package hydrus.tools

import groovy.util.logging.Slf4j
import groovyx.net.http.HttpResponseException
import hydrus.db.DBProvider
import hydrus.db.hydrus.HydrusCache
import hydrus.db.providers.SqliteProvider
import hydrus.tools.util.iqdb.IQDBService

@Slf4j
class HydrusIQDBTagger {

    public static void main(String[] args) {
        def db
        if (args.length > 0) {
            db = new SqliteProvider([db_location: args[0]])
        } else {
            db = new SqliteProvider([db_location: 'e:/Hydrus2/db/client.db'])
        }
        HydrusCache.instance.init(db)
        def iqdb = new IQDBService(IQDBService.DEFAULT_SERVICE_ADDRESS, IQDBService.DEFAULT_SERVICES, 100)
        new HydrusIQDBTagger(db, iqdb).startTagging();
    }

    private IQDBService iqdb
    private final DBProvider db

    public HydrusIQDBTagger(DBProvider db, IQDBService iqdb) {
        this.db = db
        this.iqdb = iqdb
    }

    public void startTagging() {
        //iqdb = new IQDBService('http://iqdb.harry.lu/', [])
        db.sql.eachRow('SELECT h.hash_id, h.hash, f.mime FROM files_info f JOIN hashes h ON f.hash_id = h.hash_id WHERE f.hash_id NOT IN (SELECT DISTINCT hash_id FROM mappings)') { row ->
            //db.sql.eachRow('SELECT h.hash_id, h.hash, f.mime FROM files_info f JOIN hashes h ON f.hash_id = h.hash_id WHERE f.hash_id IN (SELECT DISTINCT m.hash_id FROM mappings m JOIN tags t ON t.tag_id = m.tag_id JOIN existing_tags e ON t.tag_id = e.tag_id JOIN namespaces n ON e.namespace_id = n.namespace_id WHERE t.tag = \'e621\' AND namespace = \'sys\');') { row ->
            def unique = []
            int applied = 0
            String hash = row.hash.encodeHex().toString()

            try {
                db.sql.withTransaction {
                    def fname = "${hash.substring(0, 2)}/${hash}${MimeType.getForId(row.mime).ext}"
                    def file = 'e:/Hydrus2/db/client_files/' + fname
                    def hashId = row.hash_id
                    if (new File(hash + '.out').exists()) {
                        return
                    }
                    log.info("Tagging ${file}")
                    def similarImages = iqdb.findSimilarImages(file, 80)
                    def tags = getUniqueTags(similarImages)
                    tags.add('sys:autotagged')
                    log.info(" > tags: ${tags}")
                    tags.each {
                        String[] parts = it.toString().split(':', 2)
                        String namespace = ''
                        String tag = it.toString()
                        if (parts.size() > 1) {
                            namespace = parts[0]
                            tag = parts[1]
                        }
                        createMapping(db, hashId, HydrusCache.instance.tags.get(tag), HydrusCache.instance.namespaces.get(namespace))
                        ++applied
                    }
                    removeMapping(db, hashId, HydrusCache.instance.tags.get('fur'), HydrusCache.instance.namespaces.get('sys'))
                    log.info(" > success, applied ${applied} tags")
                }
            } catch (Exception ex) {
                log.error(" > failure at ${unique.size() > 0 ? unique[applied] : '-1'}")
                def out = new PrintStream(new File(hash + '.out').newOutputStream())
                out.println("EX ${ex.getClass()}: " + ex.getMessage())
                if (ex instanceof HttpResponseException) {
                    out.println(ex.response.allHeaders)
                }
                ex.printStackTrace(out)
                out.flush()
                out.close()
            }
        }
    }

    static List<String> getUniqueTags(List images) {
        def unique = images
                .collect { it.tags }
                .flatten()
                .unique()
        if (unique.isEmpty()) {
            unique.add('sys:tagme')
        }
        unique
    }

    static void createMapping(SqliteProvider db, long hashid, long tagid, long namespaceid) {
        db.sql.execute("INSERT OR IGNORE INTO mappings (service_id, namespace_id, hash_id, tag_id, status) VALUES (3, ${namespaceid}, ${hashid}, ${tagid}, 0)")
        db.sql.execute("INSERT OR IGNORE INTO existing_tags (namespace_id, tag_id) VALUES (${namespaceid}, ${tagid})")
    }

    static void removeMapping(SqliteProvider db, long hashid, long tagid, long namespaceid) {
        db.sql.execute("DELETE FROM mappings WHERE namespace_id=${namespaceid} AND hash_id=${hashid} AND tag_id=${tagid}")
    }


    static enum MimeType {
        APPLICATION_HYDRUS_CLIENT_COLLECTION(0, '.collection'),
        IMAGE_JPEG(1, '.jpg'),
        IMAGE_PNG(2, '.png'),
        IMAGE_GIF(3, '.gif'),
        IMAGE_BMP(4, '.bmp'),
        APPLICATION_FLASH(5, '.swf'),
        APPLICATION_YAML(6, '.yaml'),
        IMAGE_ICON(7, '.ico'),
        TEXT_HTML(8, '.html'),
        VIDEO_FLV(9, '.flv'),
        APPLICATION_PDF(10, '.pdf'),
        APPLICATION_ZIP(11, '.zip'),
        APPLICATION_HYDRUS_ENCRYPTED_ZIP(12, '.zip.encrypted'),
        AUDIO_MP3(13, '.mp3'),
        VIDEO_MP4(14, '.mp4'),
        AUDIO_OGG(15, '.ogg'),
        AUDIO_FLAC(16, '.flac'),
        AUDIO_WMA(17, '.wma'),
        VIDEO_WMV(18, '.wmv'),
        UNDETERMINED_WM(19, ''),
        VIDEO_MKV(20, '.mkv'),
        VIDEO_WEBM(21, '.webm'),
        APPLICATION_JSON(22, '.json'),
        APPLICATION_OCTET_STREAM(100, '.bin'),
        APPLICATION_UNKNOWN(101, '');

        private final String ext
        private final int ord

        MimeType(Integer ord, String ext) {
            this.ext = ext
            this.ord = ord
        }

        static MimeType getForId(int id) {
            values().find { it.ord == id }
        }

        String getExt() {
            return ext
        }
    }


}
