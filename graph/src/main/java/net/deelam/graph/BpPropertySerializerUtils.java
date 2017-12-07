package net.deelam.graph;

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Color;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.UUID;

import com.tinkerpop.blueprints.Element;

public final class BpPropertySerializerUtils {

	public static void setJavaDate(Element elem, String propName, Date date){
		checkNotNull(date);
		elem.setProperty(propName, date.getTime());
	}

	public static Date getJavaDate(Element elem, String propName){
		Long millis=(Long) elem.getProperty(propName);
		if(millis == null)
			return null;
		return new Date(millis);
	}
    
    public static void setOffsetDateTime(Element elem, String propName, OffsetDateTime date){
        checkNotNull(date);
        elem.setProperty(propName, date.toString());
    }

    public static OffsetDateTime getOffsetDateTime(Element elem, String propName){
        Object val = elem.getProperty(propName);
        String dateStr = val == null ? null : val.toString();
        if(dateStr == null)
            return null;
        return OffsetDateTime.parse(dateStr);
    }
    
	public static Color getColor(Element elem, String propName) {
		Integer rgb = elem.getProperty(propName);
		return rgb == null ? null : new Color(rgb);
	}
	
	public static void setColor(Element elem, String propName, Color color) {
		elem.setProperty(propName, color.getRGB());
	}

	public static void setUUID(Element elem, String propName, UUID uuid){
		checkNotNull(uuid);
		elem.setProperty(propName, uuid.toString());
	}

	public static UUID getUUID(Element elem, String propName){
		String uuid=elem.getProperty(propName);
		if(uuid == null)
			return null;
		return UUID.fromString(uuid);
	}
	
	
	public static void setUri(Element elem, String propName, URI uri){
		checkNotNull(uri);
		elem.setProperty(propName, uri.toString());
	}

	public static URI getUri(Element elem, String propName){
		String uri=elem.getProperty(propName);
		if(uri == null)
			return null;
		return URI.create(uri);
	}

}
