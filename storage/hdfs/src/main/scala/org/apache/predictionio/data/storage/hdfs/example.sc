import java.io.File

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import org.apache.spark.sql.functions.from_utc_timestamp

val dft = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
val t = DateTime.parse("1997-02-28 10:30:00",dft).getMillis

val dateTimeFormatter = ISODateTimeFormat.dateTime().withOffsetParsed()
val dateTimeNoMillisFormatter =
  ISODateTimeFormat.dateTimeNoMillis().withOffsetParsed()

dateTimeFormatter.print(DateTime.now())
dateTimeNoMillisFormatter.print(DateTime.now())

val timestr = "2017-08-15T09:23:04.191Z"
val t1 = DateTime.parse(timestr)

val str = None

val delimiter = str.map(x => if(x==",") "\t" else x).getOrElse("\t")


val path ="/user/albertxie/resume.pdf"
val name = path.split("/").last

val f = new File("/Users/xie/tmp/test.txt")
f.toString
val parent = new File(f.getParent)
if(!parent.exists()) parent.mkdirs()
