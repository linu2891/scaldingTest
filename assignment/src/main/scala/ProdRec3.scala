import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}
import com.twitter.scalding.FunctionImplicits._

/**
 * @author wcbdd
 */
class ProdRec3 (args:Args) extends Job(args){
  
  
   //master
  val prodCatalog = List(
                         (111,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLUE"),
                         (222,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLACK"),
                         (333,"NIKE","style","MALE","FOOTWARE","CASUALS","SHOES","BLUE"),
                         (444,"NIKE","style","FEMALE","FOOTWARE","CASUALS","SHOES","PINK")
                         )
  //sqooped data                       
  val prodRecom = List (
                         ("CLOTHING|FORMALS|SHIRT","111,222"),
                         ("CLOTHING|FORMALS|TROUSERS","444,555,666"),
                         ("FOOTWARE|CASUALS|SHOES","333")
                       )
                       
                       
// val prodCatalogSchema = List ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)
//  val input = Pipe.pipes(prodCatalog,prodCatalogSchema)
  
  
  val prodRecomPipe =     IterableSource[((String, String))](prodRecom, ('category, 'poducts)).read
  val prodPipe = IterableSource[((Int, String,String, String,String, String,String, String))](prodCatalog, ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)).read
  .filter('gender){ gender:String => gender == "MALE"}
  .project('pid,'typ1,'typ2,'typ3)  
  .map( ('typ1,'typ2,'typ3) -> 'category_){x:(String,String,String) => val(typ1,typ2,typ3) = x 
    s"$typ1|$typ2|$typ3"}
  .discard('typ1,'typ2,'typ3)  
  .joinWithSmaller('category_ -> 'category ,  prodRecomPipe)
  .project('pid,'poducts)  
  .map('poducts->'poducts_){x:String => (x.split(","))}
  .debug
  .write(Tsv( args("output")))
       
    
  def filterProducts( pid:String,products:String)={
    ("","")
    }
}