
import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}
import com.twitter.scalding.FunctionImplicits._
import scalaz.IsEmpty

/**
 * @author wcbdd
 * 
 * Re-rank the category-based recommendations based on price 
 * (defined as avg. of min/max either one nullable) asc., 
 * and retain only upto top N (say, N = 3) recommendations.
 * 
 *
 * recomm    = "CLOTHING|FORMALS|SHIRT",   "111,222,777"           
 * prodPrice = 111, 1500,1200
 */
 
 

class ProdRecByPrice (args:Args) extends Job(args){
 
import ReccomSchema._ 
import StringUtils._
                   
   
  
                                                                         
 /** read reccomendation data                                                  
  *       
  * category                   poductList
  *"CLOTHING|FORMALS|SHIRT",   "111,222,777"         
  *
  *   flatten column  TO 
  *
  * pidRecomm   Category   
  * 111,      "CLOTHING|FORMALS|SHIRT"                                                                                
  * 222,      "CLOTHING|FORMALS|SHIRT" 
  * 777,      "CLOTHING|FORMALS|SHIRT"
  */
  
  
  
  val prodRecomPipe =     Csv( args("prodRecommInput"),"," ,prodRecomSchema ).read 
  .flatMap('poducts -> 'pidRecomm){y:String => y.split(",")}
  .project('pidRecomm,'category)
                                                                   
 /**                                                                    
  * read price
  * 
  *
  * pidPrice    maxprice   'minprice
  *    111,       1500 ,     1200
  */    
  
  val prodPricePipe = Csv( args("prodPriceInput"),"," ,prodPriceSchema ).read
  
                                                                  
  /**                                                               
   * cal - average price -> ( maxPrice  + minPrice ) / 2    
   * 
   *        TO
   * pidPrice  Avgprice
   *   222,     1500
   *   111,     1250
   */
        
  .map(('maxprice, 'minprice)->('avgPrice)) {x:(String,String) => val(maxPrice,minPrice) = x 
    ((( toDouble(maxPrice) + toDouble(minPrice))/2))   
    }.project('pidPrice,'avgPrice)
                                                                
  /**                                                             
   * join Recomm pipe * ProductPrice pipe 
   *   
   *
   * pidPric,  Avgprice,    Category 
   *  222,      1500      "CLOTHING|FORMALS|SHIRT"
   *  111,      1250      "CLOTHING|FORMALS|SHIRT"
   *  
   */      
   
   .joinWithSmaller('pidPrice -> 'pidRecomm,  prodRecomPipe ) 
   .project('pidPrice,'avgPrice,'category)
   .debug
                                                               
    /**                                                           
     * sort by avgprice and take first 3                           
     */
   .groupAll { _.sortBy('avgPrice).reverse.take(3)     }
   
                                                                
    /**    
     *                                                            
     *  group by category and merge pid's 
     *  ['CLOTHING|FORMALS|SHIRT', '111,222']                                                               
     */
   .groupBy('category) { _.mkString('pidPrice, ",") }          
   .debug
   .write(Tsv( args("output")))
      
   
 
   

 
}



   //master
 /* val prodPrice = List( (111, "1500",1200F),
                        (222, "1600",1400F),
                        (777, "1200",1000F),
                        (444, "1200",1050F),
                        (333, "700",650F)
                      )*/
  
/*                      
  //sqooped data                      
  val prodRecom = List (
                         ("CLOTHING|FORMALS|SHIRT","111,222,777"),
                         ("CLOTHING|FORMALS|TROUSERS","444,555,666"),
                         ("FOOTWARE|CASUALS|SHOES","333")
                       )*/