package hydrus.tools.util.web

import groovyx.net.http.HTTPBuilder
import org.apache.http.HttpEntity
import org.apache.http.HttpRequest
import org.apache.http.HttpResponse
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.mime.content.ContentBody
import org.apache.http.entity.mime.content.StringBody
import org.apache.http.impl.client.DefaultRedirectStrategy
import org.apache.http.protocol.HttpContext
import org.codehaus.groovy.runtime.MethodClosure

class HttpClient {

    final HTTPBuilder http

    HttpClient(String baseUrl) {
        http = new HTTPBuilder(baseUrl)
        http.client.setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) {
                def redirected = super.isRedirected(request, response, context)
                return redirected || response.getStatusLine().getStatusCode() == 302
            }
        })
        http.encoder.putAt("multipart/form-data", new MethodClosure(this, 'encodeMultiPart'))
        http.setHeaders([
                'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
        ])


    }

    HttpEntity encodeMultiPart(Map<String, Object> body) {
        MultipartEntityBuilder multipartEntity = MultipartEntityBuilder.create()
        for (String key : body.keySet()) {
            Object o = body.get(key)
            if (o instanceof ContentBody) {
                multipartEntity.addPart(
                        key,
                        o
                )
            } else if (o instanceof Iterable) {
                for (def x : o) {
                    multipartEntity.addPart(key, new StringBody(x.toString()))
                }
            }
        }

        multipartEntity.build()
    }

}
