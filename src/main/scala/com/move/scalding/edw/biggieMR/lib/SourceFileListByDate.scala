package com.move.scalding.edw.biggieMR.lib

import com.mongodb.casbah.Imports._
import org.joda.time.DateTime
import org.joda.time.format._
import org.joda.convert._
import org.joda.time.Period

class SourceFileListByDate(server:String, port:String, db:String, user:String, pass:String, topic:String) {
	private val serverConn = new ServerAddress(server, port.toInt)
	private val p = pass.toCharArray
	private val credentials = MongoCredential.createMongoCRCredential(user, db, p)
	val mongoClient = MongoClient(serverConn, List(credentials))
	val dbConn = mongoClient(db)
	val col =  dbConn("c_ctrl_hdfs_file_status")
	
	def extract(start:String, end:String): List[String] ={
	   val startF = start + " 00:00:00"
	   val endF = end + " 23:59:59"
	  
	   val pathPrefix = "hdfs:///data/raw/xact/logs/edw"
	   @transient val df = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
	   @transient val startDT = df.parseDateTime(startF)
	   @transient val startM = startDT.getMillis / 1000
	   @transient val endDT = df.parseDateTime(endF)
	   @transient val endM = endDT.getMillis / 1000
	  
	   val queryCond = "source" $eq topic
	   val dateCond = {  $or (
		  $and ("start_dt" $lte startM,"end_dt" $gte startM),
		  $and ("start_dt" $lte endM,  "end_dt" $gte endM),
		  $and ("start_dt" $gte startM,"end_dt" $lte endM),
		  $and ("start_dt" $lte startM,"end_dt" $gte endM)) 
	  	}  
	   val q = queryCond ++ dateCond 
	   val returnVals = MongoDBObject("file_name" -> "1")
	  
	   val rawResult = col.find(q,returnVals).toList
	   val result = rawResult.map { x => pathPrefix +  x.getAs[String]("file_name").get }
	   return result

	}
	
	
}



