import com.twitter.scalding._
import cascading.pipe.Pipe

/**
 * @author wcbdd
 */
class ProdRecomForMen (args:Args) extends Job(args){
  
  
  val productCatalogSchema = List ('productId, 'brand, 'style,'gender,'type_p1, 'type_p2, 'subType_p3, 'color)
  val categoryRecommSchema = List ('category,'recommendations)

  
    val productCatalog = Tsv( args("inputproduct"), productCatalogSchema ).read
    val categoryRecomm = Tsv( args("inputcategory"), categoryRecommSchema ).read
    
  ///I/p 
    //productCatalog = > "PD1", "LEVIS", "SKINFSIT" , "M", "CL" , "SH" , "FR", "B"
    // categoryRecomm => "CL|SH|FR", "PD1,PD2"
    
 
    // categoryRecomm => "CL|SH|FR", "PD1,PD2" 
                                             //Step1  CL|SH|FR =>  "CL" , "SH" , "FR" , "PD1,PD2"
    
                                             //Step2  "PD1,PD2" => "CL" , "SH" , "FR" , "PD1"
                                             //                    "CL" , "SH" , "FR" , "PD2"
    
    
                                             //Step3  join productCatalog and categoryRecomm CONDITION = productId,catagory
    
    
    //.flatMap('fruits -> 'fruit){text : String => text.split(",")}
    
  
     
    
}