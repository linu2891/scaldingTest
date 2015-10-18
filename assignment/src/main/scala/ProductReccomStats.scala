
import cascading.pipe.Pipe

/**
 * @author wcbdd
 * 1. Filter the bad records and put method to filter the bad records
 * 2. Define all attrib(field names) in schema
 * 3. Declare Pipes as Pipes
 * 4. Data - Make this atleast 500
 * 5. Product Data can have more columns - Product Name , Desc - 100 [This will help to project re-ranking better]
 * 6. Unit test cases for the program - Keep in similar package as the main code
 * 7. Create package keep the code 
 * 8. Remove hard coding
 * 
 * 
 * 
 */
trait ProductReccomStats {
  
  import com.twitter.scalding.{Dsl, RichPipe}

import scala.language.implicitConversions
 import Dsl._
 import StringUtils._

  def pipe: Pipe
  val maxPrice = "maxprice"
  /**          
  *
  * flatten products list 
  *
  * Eg:  "111,222,777"  TO 
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
  .flatMap('poducts -> 'pidRecomm){recommednedProdLst:String => recommednedProdLst.split(",")}
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
  .map((maxPrice, 'minprice)->('avgPrice)) {x:(String,String) => val(maxPrice,minPrice) = x 
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
 def getTopProdsByAvgPrice(top: Int) : Pipe =
  pipe 
  .project('avgPrice,'pidPrice,'category)   
   .groupBy('category) { _.sortedReverseTake[(Double,String)](( 'avgPrice,'pidPrice) -> 'top, top) } 
   .map('top -> 'pidList){ topList : List[(Double,String)] => topList.foldLeft("")((accum,tuple) => if(accum.isEmpty())tuple._2; else accum +","+tuple._2 )}
   .project('category,'pidList)
    
     .debug   
  

  
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