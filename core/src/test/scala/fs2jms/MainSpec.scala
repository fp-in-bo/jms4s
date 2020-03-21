package fs2jms

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class MainSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  "My Code " - {
    "works" in {
      IO(1).asserting(_.shouldBe(1))
    }
  }
}
