package org.waltz.nexus;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NxdlParser {
    private final List<NxdlElement> elements = new ArrayList<>();
    private final String nxdlTemplate;

    public NxdlParser(String nxdlTemplate) {
        this.nxdlTemplate = nxdlTemplate;
    }

    public static final String ROOT_PATH = "/";

    public void parse() throws Exception {
        File xmlFile = new File(nxdlTemplate);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

        processGroup(document.getDocumentElement(), ROOT_PATH);
    }

    private void processGroup(Element node, String parentPath) {
        String groupName = node.getAttribute("name");
        String type = node.getAttribute("type");

        elements.add(new NxdlGroup(parentPath, groupName, type));

        String groupPath = parentPath == ROOT_PATH ? ROOT_PATH + groupName : parentPath + "/" + groupName;
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                Element childElement = (Element) child;
                switch (childElement.getTagName()) {
                    case "field":
                        processField(childElement, groupPath);
                        break;
                    case "group":
                        processGroup(childElement, groupPath);
                        break;
                    case "link":
                        processLink(childElement, groupPath);
                        break;
                }
            }
        }
    }

    private void processField(Element fieldElement, String parentPath) {
        String fieldName = fieldElement.getAttribute("name");
        String type = fieldElement.getAttribute("type");

        NodeList dimensions = fieldElement.getElementsByTagName("dimensions");
        long[] dims = {1};  // Default to scalar
        if (dimensions.getLength() > 0) {
            NodeList dimNodes = ((Element) dimensions.item(0)).getElementsByTagName("dim");
            dims = new long[dimNodes.getLength()];
            for (int i = 0; i < dimNodes.getLength(); i++) {
                dims[i] = Long.parseLong(((Element) dimNodes.item(i)).getAttribute("value"));
            }
        }

        elements.add(new NxdlField(parentPath, fieldName, type, dims));
    }

    private void processLink(Element linkElement, String parentPath) {
        String linkName = linkElement.getAttribute("name");
        String target = linkElement.getAttribute("target");
        elements.add(new NxdlLink(parentPath, linkName, target));
    }

    public Iterator<NxdlElement> iterator(){
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < elements.size();
            }

            @Override
            public NxdlElement next() {
                return elements.get(index++);
            }
        };
    }
}
