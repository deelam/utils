package net.deelam.graph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public final class PrettyPrintXml {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Arguments: input.xml [output.xml]");
      System.exit(1);
    }

    String inFilename = args[0];
    String outFilename = null;
    if (args.length > 1)
      outFilename = args[1];

    prettyPrint(inFilename, outFilename);
  }

  public static void prettyPrint(String inFilename, String outFilename) throws IOException, FileNotFoundException {
    try (InputStream inFile = new FileInputStream(new File(inFilename))) {
      try (Writer out = (outFilename == null) ? new StringWriter()
          : new BufferedWriter(new FileWriterWithEncoding(outFilename, StandardCharsets.UTF_8))) {
        prettyPrint(inFile, out);
        if (out instanceof StringWriter)
          System.out.println(out.toString());
      } catch (ParserConfigurationException | SAXException | TransformerFactoryConfigurationError
          | TransformerException e) {
        throw new IOException(e);
      }
    }
  }

  public static final void prettyPrint(InputStream inputStream, Writer writer) throws ParserConfigurationException,
      SAXException, IOException, TransformerFactoryConfigurationError, TransformerException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setValidating(false);
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document xml = db.parse(inputStream);

    Transformer tf = TransformerFactory.newInstance().newTransformer();
    tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
    tf.setOutputProperty(OutputKeys.METHOD, "xml");
    tf.setOutputProperty(OutputKeys.INDENT, "yes");
    tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    tf.transform(new DOMSource(xml), new StreamResult(writer));
  }

}
