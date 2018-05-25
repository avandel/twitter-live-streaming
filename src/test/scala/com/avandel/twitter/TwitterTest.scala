package com.avandel.twitter

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TwitterTest extends FlatSpec with Matchers with BeforeAndAfterAll{


  "getFinalUrl on a url without redirection" should "return url" in  {

    Twitter.getFinalUrl("http://httpbin.org/") shouldBe  Some("http://httpbin.org/")
  }

  "getFinalUrl on a url with a redirection" should "return redirected url" in  {

    Twitter.getFinalUrl("http://httpbin.org/redirect-to?url=https://httpbin.org/get") shouldBe  Some("https://httpbin.org/get")
  }

  "getFinalUrl on short url" should "return url" in  {

    Twitter.getFinalUrl("https://t.co/evIvFEbd4S") shouldBe  Some("http://europa.eu/rapid/press-release_MEMO-18-387_fr.htm")
  }

  "getFinalUrl on invalid url" should "return None" in  {

    Twitter.getFinalUrl("https://myfakeurl") shouldBe  None
  }

  "getFinalUrl on invalid url after a redirection" should "return None" in  {

    Twitter.getFinalUrl("http://httpbin.org/redirect-to?url=https://myfakeurl") shouldBe  None
  }


}
