
import com.twitter.scalding.Args
import com.twitter.scalding.Csv
import com.twitter.scalding.Job
import com.twitter.scalding.Tsv
import cascading.pipe.Pipe




/**
 * @author wcbdd
 * 
 * Re-rank the category-based recommendations based on price 
 * (defined as avg. of min/max either one nullable) asc., 
 * and retain only upto top N (say, N = 3) recommendations.
 * 
 * recomm    = "CLOTHING|FORMALS|SHIRT",   "111,222,777"           
 * prodPrice = 111, 1500,1200
 */
 
 

class ProdRecByPriceJob (args:Args) extends Job(args){

import ReccomSchema._
import ProductReccomStats._            
   
  
  val recomPipe : Pipe =     Csv( args("prodRecommInput"),"," ,PROD_RECCOM_SCHEMA ).read 
  .getReccomByProd.addTrap(Tsv( args("errorReccomRecords")))
                                                                   
 
  
  val prodPricePipe  : Pipe = Csv( args("prodPriceInput"),"," ,PROD_PRICE_SCHEMA ).read                                                        

  .calProdAvgPrice.addTrap(Tsv( args("errorPriceRecords")))    
   
  .joinWithSmaller('pidPrice -> 'pidRecomm,  recomPipe )                                                               
 
  .getTopProdsByAvgPrice(2) 
    
 
   .write(Tsv( args("output")))
      
   
   

 
}



   