/*
 * ************** 
 * Create By: Parag Jain
 * For extracting email information
 * usage -
 * javac LogEmailResponse.java
 * jar -cvf LogEmailResponse.jar LogEmailResponse.class
 * 
 * Once JAr file is ready, you can use it Tika app. Make sure the Tika app 2.0.0 is in the 
 * CLASSPATH
 * for example - export CLASSPATH="/Users/pjain/work/tika-app/tika-app-2.0.0.jar:$CLASSPATH"
 * */

import java.io.File;
import java.io.*;
import java.util.Arrays;
import org.apache.tika.Tika;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mail.RFC822Parser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class LogEmailResponse {

    public static String LogEmail(InputStream inputStream) throws IOException, TikaException, SAXException {
        
        BodyContentHandler handler = new BodyContentHandler();
        Metadata tikaMetadata = new Metadata();
        Parser paras = new  RFC822Parser();

        try { 

            paras.parse(inputStream, handler, tikaMetadata, new ParseContext());

            String fromField = tikaMetadata.get(tikaMetadata.MESSAGE_FROM);
            String subField = tikaMetadata.get("dc:subject");

            String bodyText = handler.toString().replaceAll("[\\n\\t ]", " ");

            String emailInfo = fromField + "|" + subField + "|" + bodyText;

            return emailInfo;

        }
        catch (SAXException exception){
            System.out.println("Exception thrown :" + exception.toString());
            throw exception;
        }

        
    }

}