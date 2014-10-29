package com.move.scalding.edw.biggieMR

import com.twitter.scalding._

class AndriodUsers(args : Args) extends InstrumentationRawByDate(args) {
  
	val input = rawInstrumentationSchema //rawInstrumentation
			.limit(10)
	
	input.write(output)
}
