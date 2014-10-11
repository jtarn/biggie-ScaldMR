package com.move.scalding.edw.biggieMR

import com.twitter.scalding._
import org.joda.time.DateTime
import org.joda.time.format._
import org.joda.convert._
import org.joda.time.Period

abstract class InstrumentationRawByDate(args : Args) extends Job(args) {
	
	val output = TextLine(args("output"))	
	val mongoServer = args("mongoServer")
	val mongoDb = args("mongoDb")
	val mongoPort = args("mongoPort")
	val mongoUser = args("mongoUser") 
	val mongoPass = args("mongoPass")
	val startDt = args("startDt")
	val endDt = args("endDt")
	val topic = "instrumentation"
	
	val fileSchema: cascading.tuple.Fields = ('logdate, 'logtime, 's_sitename, 's_ip, 'cs_method, 'cs_uri_stem,'cs_uri_query, 's_port, 'cs_username, 'c_ip, 'cs_version, 'cs_user_agent, 'cs_cookie, 'cs_referer, 'cs_host, 'sc_status, 'sc_substatus, 'sc_win32_status, 'time_taken)
		
	def parseInputs(in: Array[String]) = {
		if (in.length != 19) None
		else Some((in(0),in(1),in(2),in(3),in(4),in(5),in(6),in(7),in(8),in(9),in(10),in(11),in(12),in(13),in(14),in(15),in(16),in(17),in(18)))
	}

	//  get sourcefile list
	@transient val slo = new SourceFileListByDate(mongoServer, mongoPort, mongoDb, mongoUser, mongoPass, topic)	
	@transient val sl = slo.extract(startDt,endDt)
	slo.mongoClient.close

	// get start and end dates
	@transient val df = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
	@transient val logDf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z")
	val firstEventDtM = df.parseDateTime(startDt + " 00:00:00").getMillis
	val lastEventDtM = df.parseDateTime(endDt + " 23:59:59").getMillis

	val rawInstrumentationInput = MultipleTextLineFiles(p = sl : _*)
	val rawInstrumentationSchema = rawInstrumentationInput
		.read 
		.filter('line) { record:String => record.substring(0,1) != "#" }
		.flatMapTo (('line -> fileSchema)) { line : (String) =>  
		  val f = parseInputs(line.split(" "))
		  f match {
		    case Some(_) => f
		    case None => None // do some logging here
		  }
		}
	val rawInstrumentation = rawInstrumentationSchema.filter('date, 'time) { dt : (String, String) =>
	  val (date, time) = dt
	  val eventDtRaw = date + " " + time + " UTC" 
	  val eventDt = logDf.parseDateTime(eventDtRaw) 
	  val eventDtM = eventDt.getMillis
	  (eventDtM <= lastEventDtM) && (eventDtM >= firstEventDtM)
	}

			
  
}