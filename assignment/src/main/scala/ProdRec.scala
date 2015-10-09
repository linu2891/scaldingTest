import com.twitter.scalding._
import scala.util.matching.Regex
import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}

class ProdRec (args: Args) extends Job(args){
  
  val prodCatalog = List(
                         (111,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLUE"),
                         (222,"LEVIS","style","FEMALE","CLOTHING","FORMALS","SHIRT","PINK"),
                         (333,"NIKE","style","MALE","FOOTWARE","CASUALS","SHOES","BLUE")
                         )
                         
  val prodRecom = List (
                         ("CLOTHING-FORMALS-SHIRT","111,222"),
                         ("CLOTHING-FORMALS-TROUSERS","444,555,666"),
                         ("FOOTWARE-CASUALS-SHOES","333")
                       )
  
  val prodPipe = IterableSource[((Int, String,String, String,String, String,String, String))](prodCatalog, ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)).read
  
   val prodRecomPipe =
    IterableSource[((String, String))](prodRecom, ('category, 'poducts)).read
    
      .flatMapTo('poducts -> ('pid1)) { text : String => text.split(",") } // (p1,"CL-FM-ST"),(p2,"CL-FM-ST")
      .mapTo('pid1-> 'pid2) {pid : Int => pid.toInt}
      .joinWithSmaller('pid2 -> 'pid, prodPipe)
      .discard('pid)
      .filter('gender){f:String => f == "MALE"}      
      .debug
      .write( TextLine( args("output")))

  def splitRecomm(rec: String) = {
    (rec.split("-").toList.get(0), // 1st tuple element
      rec.split("-").toList.get(1), // 2nd tuple element
      rec.split("-").toList.get(2)) // 3rd tuple element
  }

  
      
     // [product15] 10.0 11
}