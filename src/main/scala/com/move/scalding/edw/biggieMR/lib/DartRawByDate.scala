package com.move.scalding.edw.biggieMR

import com.twitter.scalding._
import cascading.tuple.Fields
import scala.Some
import org.joda.time.DateTime
import org.joda.time.format._
import org.joda.convert._
import org.joda.time.Period
import com.move.scalding.edw.biggieMR.lib.ParseUtilities
import com.move.scalding.edw.biggieMR.lib.SourceFileListByDate


abstract class DartRawByDate (args : Args) extends Job(args) {
//      File contents:
//	    'Time, 'UserId, 'AdvertiserId, 'OrderId, 
//	    'LineItemId, 'CreativeId, 'CreativeVersion, 
//	    'CreativeSize, 'AdUnitId, 'CustomTargeting, 'Domain, 
//	    'CountryId, 'Country, 'RegionId, 'Region, 'MetroId, 
//	    'Metro, 'CityId, 'City, 'PostalCodeId, 'PostalCode, 
//	    'BrowserId, 'Browser, 'OSId, 'OS, 'OSVersion, 'BandWidth, 
//	    'TimeUsec, 'AudienceSegmentIds, 'Product, 'RequestedAdUnitSizes, 
//	    'BandwidthGroupId, 'MobileDevice, 'MobileCapability, 
//	    'MobileCarrier, 'IsCompanion, 'PublisherProvidedID
  
  
//  scala tuple only allows for 22 fields.   Some fields are grouped together as Map() based on overall content  
  	val fileSchema : cascading.tuple.Fields = ('Time,'UserId, 'adInfo,'CustomTargeting, 'Domain, 
  	    'userLocation,'browerInfo,'BandWidth, 'TimeUsec, 'AudienceSegmentIds, 
  	    'Product, 'RequestedAdUnitSizes, 'BandwidthGroupId, 'mobileInfo, 
  	    'IsCompanion, 'PublisherProvidedID)

	val output = TextLine(args("output"))	
	val mongoServer = args("mongoServer")
	val mongoDb = args("mongoDb")
	val mongoPort = args("mongoPort")
	val mongoUser = args("mongoUser") 
	val mongoPass = args("mongoPass")
	val startDt = args("startDt")
	val endDt = args("endDt")
	val topic = "instrumentation"
	  	    
	    	    
	//  get sourcefile list
	@transient val slo = new SourceFileListByDate(mongoServer, mongoPort, mongoDb, mongoUser, mongoPass, topic)	
	@transient val sl = slo.extract(startDt,endDt)
	slo.mongoClient.close

	// get start and end dates
	@transient val df = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
	@transient val logDf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z")
	val firstEventDtM = df.parseDateTime(startDt + " 00:00:00").getMillis
	val lastEventDtM = df.parseDateTime(endDt + " 23:59:59").getMillis

	val rawDartInput = MultipleTextLineFiles(p = sl : _*)
	 
	 val parsedDartInput = rawDartInput.read
	 	.flatMapTo(('line -> fileSchema )){ line : String =>  
//	 	  val tst = line.map (c => c.toInt.toString + " ").mkString  
//	 	  println(tst)
		  val f = ParseUtilities.parseDartInputs(line.split(65533.asInstanceOf[Char]))  
		  // apparently this "65533" char is different per OS.  This is the unicode thorn that would not split via unicode escape or octal. run a sample (like above
		  // to explore the chars) if you have problems
		  f
		}

	val filHeaderDartInput = parsedDartInput
		.filter('Time) {t:String =>
		  t != "Time"
		    }
	
	val filterTimeDartInput = filHeaderDartInput
		.filter('TimeUsec) { t : String =>
		val dLg =  try {
		  (t.toLong * 1000)
		} catch {
		  case e: Exception => 
		    "0".toLong
		}
		val dt = new DateTime(dLg).getMillis
		(dt <= lastEventDtM) && (dt >= firstEventDtM)
	}
	
	val custTargetingDart = filterTimeDartInput
			.map('CustomTargeting -> 'CustomTargeting) { q :String => 
			val qRes = ParseUtilities.parseQueryParms(q, ";","=")
			qRes
	}
	
	val rawDart = custTargetingDart
	
}