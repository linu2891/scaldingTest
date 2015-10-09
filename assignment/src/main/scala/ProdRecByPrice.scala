
import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}
import com.twitter.scalding.FunctionImplicits._

/**
 * @author wcbdd
 */
class ProdRecByPrice (args:Args) extends Job(args){
 
 
 
   //master
  val prodPrice = List( (111, "1500",1200F),
                        (222, "1600",1400F),
                        (777, "1200",1000F),
                        (444, "1200",1050F),
                        (333, "700",650F)
                      )
                      /*
  //sqooped data                      
  val prodRecom = List (
                         ("CLOTHING|FORMALS|SHIRT","111,222,777"),
                         ("CLOTHING|FORMALS|TROUSERS","444,555,666"),
                         ("FOOTWARE|CASUALS|SHOES","333")
                       )*/
                      
   
 val prodRecomSchema = List('category, 'poducts)                       
 
 
  val prodRecomPipe =     Csv( args("prodRecommInput"),"," ,prodRecomSchema ).read 
  .flatMap('poducts -> 'pid_){y:String => y.split(",")}
  .map('pid_ -> 'pid__){x:String => x.toInt}  //pid , category
  
  val prodPricePipe = IterableSource[((Int,String,Float))](prodPrice, ('pid,'maxprice, 'minprice)).read 
  .map(('maxprice)->('maxprice_)) { x:String => {  ( x.toFloat ) }}
 
  .map(('maxprice_, 'minprice)->('avgPrice)) {x:(String,String) => val(maxPrice,minPrice) = x 
    (( maxPrice.toFloat + minPrice.toFloat)/2)    //average price
    }
   .joinWithSmaller('pid -> 'pid__,  prodRecomPipe) //join Recomm => ProductPrice
   .debug
    //sortby avgPrice and create List-> 'top
   .groupAll { group => group.sortedReverseTake [(Float,String,String)] (('avgPrice,'pid,'category)->'top, 3) }
   .debug
   .flattenTo[((Float,String,String))]('top -> ('avgPrice,'pid,'category))
   .debug
   .groupBy('category) { _.mkString('pid, ",") }   
   .debug
  .write(Tsv( args("output")))
      
   
 
}

//  .map(('maxprice)->('maxprice_)) { x:String => {  ( x.toFloat ) }}
//  .map(('minprice)->('minprice_)) { x:String => {  ( x.toFloat ) }}
// def toFloat(maxP:String,minP:String):(Float,Float)={
//      
//     ((""+maxP).toFloat , (""+minP).toFloat)
//   }
///val prodPriceSchema = List('pid,'maxprice, 'minprice)