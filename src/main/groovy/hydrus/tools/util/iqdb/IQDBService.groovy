package hydrus.tools.util.iqdb

import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.HttpResponseException
import hydrus.tools.util.web.HtmlParser
import hydrus.tools.util.web.HttpClient
import org.apache.commons.imaging.Imaging
import org.apache.commons.lang.StringUtils
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.entity.mime.content.StringBody
import org.imgscalr.Scalr

import javax.imageio.ImageIO
import java.awt.image.BufferedImage

@Slf4j
class IQDBService {

    public static final Integer DEFAULT_SCALE_MAX_PX_SIZE = 100
    public static final String DEFAULT_SERVICE_ADDRESS = 'http://iqdb.org/'
    // FIXME the commented out are either useless or require authentication - add authentication option
    public static final List<Integer> DEFAULT_SERVICES = [1, 2, 3, 4, 5, 6, /*10,*/ /*11,*/ 12/*, 13*/]

    private final String serviceAddress
    private final int scaleMaxSize
    private List<Integer> services;

    public IQDBService(String serviceAddress = DEFAULT_SERVICE_ADDRESS, List<Integer> services = DEFAULT_SERVICES, Integer scaleMaxSize = DEFAULT_SCALE_MAX_PX_SIZE) {
        this.scaleMaxSize = scaleMaxSize
        this.services = services
        this.serviceAddress = serviceAddress
    }

    public def findSimilarImages(String filename, Integer similarityThreshold = 80) {
        def client = new HttpClient(serviceAddress)
        def is = getImageAsPngInputStream(readScaledImage(filename, scaleMaxSize, scaleMaxSize))

        client.http.post(
                path: '/',
                requestContentType: "multipart/form-data",
                body: [
                        MAX_FILE_SIZE: new StringBody('8388608'),
                        file         : new InputStreamBody(is, 'image/png', '1.png'),
                        'service[]'  : services.isEmpty() ? [0] : services
                ],
                headers: [
                        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ]
        ) { HttpResponseDecorator resp ->
            def ret = processResponse(resp, similarityThreshold)
            ret
        }
    }

    private static def processResponse(HttpResponseDecorator resp, Integer similarityThreshold) {
        assert resp.statusLine.statusCode == 200
        def html = resp.entity.content.text
        def root = new HtmlParser().parseText(html)

        def imageContainers = root.'**'.findAll {
            it instanceof Node && 'td'.equals(it.name()) && 'image'.equals(it.'@class') && it.a[0] != null
        }
        assert !imageContainers.isEmpty()

        // FIXME try to use iqdb provided (as an img.@alt attribute) tags
        // ie.:  ... <img src="http://iqdb.org/danbooru/2/9/6/296e60be7495e9aa1995aadf11550ce7.jpg" alt="Rating: s
        // Score: 1 Tags: 1girl ^_^ chroma_(chroma000) closed_eyes fingers_to_cheeks goggles goggles_on_head green_hair
        // grin gumi headphones looking_at_viewer short_hair short_hair_with_long_locks smile solo tagme vocaloid"
        // title="Rating: s Score: 1 Tags: 1girl ^_^ chroma_(chroma000) closed_eyes fingers_to_cheeks goggles
        // goggles_on_head green_hair grin gumi headphones looking_at_viewer short_hair short_hair_with_long_locks smile
        // solo tagme vocaloid">
        //
        // Try to use this instead of asking "external" services
        def imgs = imageContainers.collect {
            def url = it.a[0].@href
            def similarity = getSimilarity(it)
            [url: url, similarity: similarity]
        }
        imgs = imgs.findAll { it.similarity >= similarityThreshold }.sort { -it.similarity }

        GParsPool.withPool {
            imgs.collectParallel {
                def tags = getTags(it.url);
                [tags: tags, url: it.url, similarity: it.similarity]
            }
        }
    }

    private static def getTags(String imgUrl) {
        log.info(" > checking ${imgUrl}")
        def tags
        // FIXME in those methods use APIs wherever possible
        if (imgUrl.contains('sankakucomplex')) {
            tags = getSankakuComplexTags(imgUrl)
        } else if (imgUrl.contains('danbooru.donmai.us')) {
            tags = getDanbooruTags(imgUrl)
        } else if (imgUrl.contains('yande.re')) {
            tags = getYandereTags(imgUrl)
        } else if (imgUrl.contains('gelbooru.com')) {
            tags = getGelbooruTags(imgUrl)
        } else if (imgUrl.contains('konachan.com')) {
            tags = getKonachanTags(imgUrl)
        } else if (imgUrl.contains('e621.net')) {
            tags = getE621Tags(imgUrl)
        } else {
            log.error(" > unknown site: $imgUrl")
            tags = ['sys_unknwn_site:imgUrl']
        }
        return tags.findAll { !StringUtils.isEmpty(it) }.collect {
            it.toString().replaceAll(/_/, ' ').toLowerCase().trim()
        }
    }

    private static def transformNamespaces(List<String> tagList) {
        def namespaceSynonyms = [
                'character': ['category-4', 'tag-type-character', 'character'],
                'series'   : ['category-3', 'tag-type-copyright', 'copyright'],
                'artist'   : ['category-1', 'tag-type-artist', 'artist'],
                'medium'   : ['tag-type-medium'],
                'circle'   : ['circle'], // ??? yande.re
                'faults'   : ['faults'], // ??? yande.re
                'meta'     : ['tag-type-meta'], // ??? sankaku
                'species'  : ['tag-type-species'], // e621
                'style'    : ['style'],
                ''         : ['category-0', 'general', 'tag-type-general']
        ]
        tagList.collect { tagWithNamespace ->
            String ret = tagWithNamespace
            if (tagWithNamespace.contains(':')) {
                String[] parts = tagWithNamespace.split(/:/, 2)
                String namespace = parts[0]
                String tag = parts[1]
                for (String k : namespaceSynonyms.keySet()) {
                    if (namespaceSynonyms[k].contains(namespace)) {
                        if (!StringUtils.isBlank(k)) {
                            ret = "${k}:${tag}"
                            break
                        } else {
                            ret = tag
                        }
                    }
                }
            }
            ret
        }
    }

    private static def getSankakuComplexTags(String url) {
        // FIXME sankaku likes to return 429 Too Many Requests, this is temporary workaround until I figure out something better
        sleep(5000)
        httpRequest(url, { html ->
            def ret = []
            def root = new HtmlParser().parseText(html)
            def tagSidebar = root.'**'.find {
                it instanceof Node && 'ul'.equals(it.name()) && 'tag-sidebar'.equals(it.@id)
            }
            transformNamespaces(tagSidebar.li.collect {
                "${it.@class.toString()}:${it.a[0].value()[0]}"
            })
        }, { [] })
    }

    private static def getDanbooruTags(String url) {
        httpRequest(url, { html ->
            def root = new HtmlParser().parseText(html)
            def tagList = root.'**'.findAll {
                it instanceof Node && 'li'.equals(it.name()) && it?.@class?.toString()?.matches(/category-\d+/)
            }
            transformNamespaces(tagList.collect {
                "${it.@class.toString()}:${it.a[1].value()[0]}"
            })
        }, { [] })
    }

    private static def getYandereTags(String url) {
        httpRequest(url, { html ->
            def root = new HtmlParser().parseText(html)
            def tagSidebar = root.'**'.find {
                it instanceof Node && 'ul'.equals(it.name()) && 'tag-sidebar'.equals(it.@id)
            }
            transformNamespaces(tagSidebar.li.collect {
                "${it.'@data-type'.toString()}:${it.a[1].value()[0]}"
            })
        }, { [] })
    }

    private static def getGelbooruTags(String url) {
        httpRequest(url, { html ->
            def root = new HtmlParser().parseText(html)
            def tagSidebar = root.'**'.find {
                it instanceof Node && 'ul'.equals(it.name()) && 'tag-sidebar'.equals(it.@id)
            }
            transformNamespaces(tagSidebar.li.collect {
                "${it.@class.toString()}:${it.a[1].value()[0]}"
            })
        }, { [] })
    }

    private static def getKonachanTags(String url) {
        getYandereTags(url) // same structure
    }

    private static def getE621Tags(String url) {
        httpRequest(url, { html ->
            def root = new HtmlParser().parseText(html)
            def tagSidebar = root.'**'.find {
                it instanceof Node && 'ul'.equals(it.name()) && 'tag-sidebar'.equals(it.@id)
            }
            transformNamespaces(tagSidebar.li.findAll { it.@id == null }.collect {
                "${it.@class.toString()}:${it.a[1].value()[0]}"
            })
        }, { [] })
    }

    private static def httpRequest(String url,
                                   Closure onSuccess,
                                   Closure onFailure = { throw it },
                                   boolean failOnTooManyRequests = false) {
        try {
            def client = new HttpClient(url)
            client.http.get([:]) { HttpResponseDecorator resp ->
                assert resp.statusLine.statusCode == 200
                onSuccess.call(resp.entity.content.text)
            }
        } catch (Exception ex) {
            if (ex instanceof HttpResponseException && ex.statusCode == 429) {
                if (!failOnTooManyRequests) { // received 429 Too Many Requests, wait 1-6s and try again
                    sleep(1000 + new Random().nextInt(5000))
                    httpRequest(url, onSuccess, onFailure, true)
                } else { // tag the file so I know the tags couldn't be downloaded due to 429
                    return ['sys_429:' + url]
                }
            }
            onFailure.call(ex)
        }
    }

    private static def getSimilarity(def it) {
        def tbody = it.parent().parent()
        String similarity = tbody.'**'.find {
            it instanceof Node && it.value()[0] instanceof String && it.value()[0].matches(/\d+% similarity/)
        }.value()[0].toString()
        Integer.parseInt((similarity =~ /(\d+)% similarity/)[0][1] as String)
    }

    private static def getImageAsPngInputStream(BufferedImage image) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(image, 'png', os);
        new ByteArrayInputStream(os.toByteArray())
    }

    private static def readScaledImage(String f, int w, int h) {
        BufferedImage img;
        try {
            img = Imaging.getBufferedImage(new File(f))
        } catch (Exception ignored) {
            img = ImageIO.read(new File(f))
        }
        Scalr.resize(img, Scalr.Method.QUALITY, Scalr.Mode.AUTOMATIC, w, h, Scalr.OP_ANTIALIAS);
    }

}
