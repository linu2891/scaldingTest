package shoes

import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}
import com.twitter.scalding.FunctionImplicits._
import scala.collection.mutable.ArrayBuffer

/**
 * @author wcbdd
 */
class ProdRec3 (args:Args) extends Job(args){
 
  
     //master
 /* val prodCatalog = List(
                         (111,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLUE"),
                         (222,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLACK"),
                         (333,"NIKE","style","MALE","FOOTWARE","CASUALS","SHOES","BLUE"),
                         (444,"NIKE","style","FEMALE","FOOTWARE","CASUALS","SHOES","PINK"),
                         (777,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","GREEN")
                         )*/
  //sqooped data                      
 /* val prodRecom = List (
                         ("CLOTHING|FORMALS|SHIRT","111,222,777"),
                         ("CLOTHING|FORMALS|TROUSERS","444,555,666"),
                         ("FOOTWARE|CASUALS|SHOES","333")
                       )*/
                      
 val prodCatalogSchema = List ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)
 val prodRecomSchema = List('category, 'poducts)
 
 
 
 val prodPipe = Csv( args("prodCatalogInput"),"," ,prodCatalogSchema ).read
 
  val prodRecomPipe = Csv( args("prodRecommInput"),"," ,prodRecomSchema ).read   
   prodPipe                                                                         
  .filter('gender){ f:String => f == "MALE"}
  .project('pid,'typ1,'typ2,'typ3) 
  .map( ('typ1,'typ2,'typ3) -> 'category_){x:(String,String,String) => val(typ1,typ2,typ3) = x  //create category "typ1|typ2|typ3"
    s"$typ1|$typ2|$typ3"}
  .discard('typ1,'typ2,'typ3) 
  .joinWithSmaller('category_ -> 'category ,  prodRecomPipe)  //join  "prodCatalog  and   prodRecom"  on category
  .project('pid,'poducts) 
  .map(('pid,'poducts)->('pid,'poducts)){x:(String,String) => val (pid, poducts)= x
       filterProducts(pid, poducts)
    }
  .debug
  .write(Tsv( args("output")))
      
   
  def filterProducts( pid:String,products:String):(String,String)={
    if(products.contains(pid))
    {
      var prodArray = products.split(",")
       val b = prodArray.filter(! _.contains(pid))
      
     val newProducts  = b.mkString(",")
      (pid,newProducts)
    }
    else
      (pid,products)
    }
  


}

//  def stripChars(s:String, ch:String)= s filterNot (ch contains _)



// =     IterableSource[((String, String))](prodRecom, ('category, 'poducts)).read

//= IterableSource[((Int, String,String, String,String, String,String, String))](prodCatalog, ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)).read