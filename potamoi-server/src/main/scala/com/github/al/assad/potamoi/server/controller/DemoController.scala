package com.github.al.assad.potamoi.server.controller

import org.springframework.web.bind.annotation.{GetMapping, RestController}

@RestController
class DemoController {

  @GetMapping(Array("/hello"))
  def hello(): String = {
    "hi"
  }

}
