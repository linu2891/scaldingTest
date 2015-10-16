
import cascading.pipe.Pipe

/**
 * @author wcbdd
 */
trait ProductReccomStats {
  
  import com.twitter.scalding.{Dsl, RichPipe}

import scala.language.implicitConversions
 import Dsl._
 import StringUtils._

  def pipe: Pipe
  
  /**          
  *
  * flatten products list "111,222,777"  TO 
  *
  * pidRecomm   Category   
  * 111,      "CLOTHING|FORMALS|SHIRT"                                                                                
  * 222,      "CLOTHING|FORMALS|SHIRT" 
  * 777,      "CLOTHING|FORMALS|SHIRT"
 *
 * INPUT_SCHEMA: PROD_RECCOM_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 */
 def getReccomByProd : Pipe =
  pipe 
  .flatMap('poducts -> 'pidRecomm){y:String => y.split(",")}
  .project('pidRecomm,'category)
  
  
 /**
 * Calculates the average price -  average price -> ( maxPrice  + minPrice ) / 2  
 *   pidPrice  Avgprice
 *     222,     1500
 *     111,     1250
 *
 * INPUT_SCHEMA: PROD_PRICE_SCHEMA
 * OUTPUT_SCHEMA: PROD_AVG_PRICE_SCHEMA
 */
 def calProdAvgPrice : Pipe =
  pipe        
  .map(('maxprice, 'minprice)->('avgPrice)) {x:(String,String) => val(maxPrice,minPrice) = x 
    ((( toDouble(maxPrice) + toDouble(minPrice))/2))   
    }.project('pidPrice,'avgPrice)
    
  
    

  
  
   /**          
  *
  * flatten products list "111,222,777"  TO 
  *
  * pidRecomm   Category   
  * 111,      "CLOTHING|FORMALS|SHIRT"                                                                                
  * 222,      "CLOTHING|FORMALS|SHIRT" 
  * 777,      "CLOTHING|FORMALS|SHIRT"
 *
 * INPUT_SCHEMA: PROD_RECCOM_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 */
 def getTopProdsByAvgPrice : Pipe =
  pipe 
  .project('pidPrice,'avgPrice,'category)   
   .groupAll { _.sortBy('avgPrice).reverse.take(3)     }
  
 /**          
 *
 * Get top products based on average price 
 * INPUT_SCHEMA: PROD_AVG_PRICE_SCHEMA, RECCOM_BY_PRODUCT_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 */
 def getTopProdsByAvgPrice(prodPricePipe: Pipe, prodReccomPipe: Pipe, top:Int) : Pipe = {
  
    prodPricePipe.joinWithSmaller('pidPrice -> 'pidRecomm,  prodReccomPipe ) 
   .project('pidPrice,'avgPrice,'category)
   .debug                                                               
    /**                                                           
     * sort by avgprice and take first 3                           
     */
   .groupAll { _.sortBy('avgPrice).reverse.take(top)     }
  }
  
  
 /**          
 *   
 *                                                            
 *  group by category and merge pid's 
 *  ['CLOTHING|FORMALS|SHIRT', '111,222']                                    
 
 *
 * INPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCTS_SCHEMA
 */
 def getReccomProdList : Pipe =
  pipe 
  .groupBy('category) { _.mkString('pidPrice, ",") }          
   .debug
   
  
}

/**
 * @author wcbdd
 */
object  ProductReccomStats {
  implicit class ProductReccomStatsWrapper(val pipe: Pipe) extends AnyRef with ProductReccomStats
}