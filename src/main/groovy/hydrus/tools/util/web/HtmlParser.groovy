package hydrus.tools.util.web

import org.cyberneko.html.parsers.SAXParser

class HtmlParser {

    private final XmlParser parser

    HtmlParser() {
        def saxParser = new SAXParser()
        saxParser.setProperty('http://cyberneko.org/html/properties/names/elems', 'lower')
        parser = new XmlParser(saxParser)
    }

    def parse(String url) {
        parser.parse(url)
    }

    def parseText(String text) {
        parser.parseText(text)
    }

}
