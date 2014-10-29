package com.move.scalding.edw.biggieMR

import com.twitter.scalding._
import org.joda.time.DateTime
import org.joda.time.format._
import org.joda.convert._
import org.joda.time.Period
import com.move.scalding.edw.biggieMR.lib.ParseUtilities
import com.move.scalding.edw.biggieMR.lib.SourceFileListByDate

abstract class InstrumentationRawTest(args : Args) extends Job(args) {
	
	
	val fileSchema: cascading.tuple.Fields = ('logdate, 'logtime, 's_sitename, 's_ip, 'cs_method, 
	    'cs_uri_stem,'cs_uri_query, 's_port, 'cs_username, 'c_ip, 'cs_version, 'cs_user_agent, 
	    'cs_cookie, 'cs_referer, 'cs_host, 'sc_status, 'sc_substatus, 'sc_win32_status, 'time_taken)

	val rawInstrumentationInput = TextLine(args("input"))

	val rawInstrumentationSchema = rawInstrumentationInput
		.read 
		.filter('line) { record:String => record.substring(0,1) != "#" }
		.flatMapTo (('line -> fileSchema)) { line : (String) =>  
		  val f = ParseUtilities.parseInstrumentationInputs(line.split(" "))
		  f match {
		    case Some(_) => f
		    case None => None // do some logging here
		  }
		}

	val rawInstrumentation = rawInstrumentationSchema
		.map ('cs_uri_query -> 'cs_uri_query) { q :String => 
			val qRes = ParseUtilities.parseQueryParms(q, "&","=")
			qRes
		}
		

			
  
}