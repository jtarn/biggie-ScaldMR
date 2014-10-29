package com.move.scalding.edw.biggieMR.lib

import java.net.URLDecoder

class ParseUtilities {

}

object ParseUtilities {
  
  def parseQueryParms(query:String, tagDelim:String, valDelim:String) = {
  			val qSplit = query.split(tagDelim)
			val qValSplit = qSplit.map { tagPair:String =>
				val tagList = tagPair.split(valDelim)
				val tagTup = 
				  if (tagList.length == 1) (tagList(0),None)
				  else (tagList(0),  URLDecoder.decode(tagList(1), "UTF-8"))
				tagTup
				}
			val qMap = qValSplit.toMap
			qMap
  }
  
//      Parsing out Dart contents  
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
  
  
//  scala tuple only allows for 22 fields.   Some fields are grouped together based on overall content  
//  	val fileSchema : cascading.tuple.Fields = ('Time,'UserId, 'adInfo,'CustomTargeting, 'Domain, 
//  	    'userLocation,'browerInfo,'BandWidth, 'TimeUsec, 'AudienceSegmentIds, 
//  	    'Product, 'RequestedAdUnitSizes, 'BandwidthGroupId, 'mobileInfo, 
//  	    'IsCompanion, 'PublisherProvidedID)
//
//  	    
	def parseDartInputs(in: Array[String]) = { // up to 37 fields in array
  		// add some extra array positions if missing using Nones
		val extra = ((in.length-1) to 36).map  (e => "") // 36 as max array pos
		val fields = (in ++ extra).map( s => s.toString)  // None.toString is a String:None
		val adInfo = Map(
				"AdvertiserId" -> fields(2), 
				"OrderId" -> fields(3), 
				"LineItemId" -> fields(4), 
				"CreativeId" -> fields(5), 
				"CreativeVersion" -> fields(6), 
				"CreativeSize" -> fields(7), 
				"AdUnitId" -> fields(8)
				)
		val userLocation = Map(
			    "CountryId" -> fields(11),
				"Country" -> fields(12),
				"RegionId" -> fields(13),
				"Region" -> fields(14),
				"MetroId" -> fields(15),
				"Metro" -> fields(16),
				"CityId" -> fields(17),
				"City" -> fields(18),
				"PostalCodeId" -> fields(19),
				"PostalCode" -> fields(20)
				)
		val browserInfo = Map(
				"BrowserId" -> fields(21),
				"Browser" -> fields(22),
				"OSId" -> fields(23),
				"OS" -> fields(24),
				"OSVersion" -> fields(25)
				)
		val mobileInfo = Map(
				"MobileDevice" -> fields(32), 
				"MobileCapability" -> fields(33), 
				"MobileCarrier" -> fields(34)
				)
		val dartD = Some((fields(0), fields(1),adInfo,fields(9),fields(10),
		    userLocation,browserInfo,fields(26),fields(27),fields(28),fields(29),
		    fields(30),fields(31),mobileInfo,fields(35),fields(36) ) )
		dartD
  	}
	
	
	def parseInstrumentationInputs(in: Array[String]) = {
		if (in.length != 19) None
		else Some((in(0),in(1),in(2),in(3),in(4),in(5),in(6),in(7),in(8),in(9),in(10),in(11),in(12),in(13),in(14),in(15),in(16),in(17),in(18)))
	}
}

