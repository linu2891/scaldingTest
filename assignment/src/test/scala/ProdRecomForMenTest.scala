import com.twitter.scalding._
import org.scalatest._

/**
 * @author wcbdd
 */
class ProdRecomForMenTest extends WordSpec with Matchers{
  
   // Import Dsl._ to get implicit conversions from List[Symbol] -> cascading.tuple.Fields etc
  import Dsl._

  val productCatalogSchema = List ('productId, 'brand, 'style, 'gender,
    'type_p1, 'type_p2, 'subType_p3, 'color)
  val categoryRecommSchema = List ('category,'recommendations) //(category=>format: p1|p2|p3 ,recommendations=>format:pd1,pd2 )
    
  val testDataPC = List(
    ("PD1", "LEVIS", "SKINFIT" , "M", "CL" , "SH" , "FR", "B"),   //CL -CLOTHING, SH-SHIRTS, FR-FORMALS
    ("PD2", "LEVIS", "SKINFIT" , "F", "CL" , "SH" , "CA", "P"),   //CL -CLOTHING, SH-SHIRTS, CA-CASUALS
    ("PD3", "LEVIS", "SKINFIT" , "M", "CL" , "TR" , "FR", "R"),
    ("PD4", "LEVIS", "SKINFIT" , "M", "CL" , "TR" , "FR", "P"))   //CL -CLOTHING, TR-TROUSERS, FR-FORMALS
    
  val testDataCatRec = List(
    ("CL|SH|FR", "PD1,PD2"), //cat=> 
    ("CL|TR|FR", "PD3,")
    )
    
    //RECOMENDED PRODUCTS FOR MEN SHOULD BE
    //PD1 , "LEVIS", "SKINFIT","B"
    //PD3 , "LEVIS", "SKINFIT","R"

//
//  "The LoginGeo job" should {
//      JobTest("ProdRecomTest")
//        .arg("input", "inputFile")
//        .arg("output", "outputFile")
//        .source(Tsv("inputFile", schema), testData )
//        .sink[(String)](Tsv("outputFile")){
//           outputBuffer => val result = outputBuffer.mkString
//           "identify nearby login events and bucket them" in {
//             result shouldEqual s"""[{"lat":40.00,"lon":30.00,"device":"PC",count:2}]"""
//           }
//        }.run
//         .finish
//    }
  
}