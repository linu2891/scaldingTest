package shoes
import com.twitter.scalding._
/**
 * @author t
 */
trait ProdReccomTrait[T,V] {
  
  def filterSelfProduct(pid:T, products:V)
}