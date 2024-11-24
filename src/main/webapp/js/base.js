(function($){

$("#buttons").on("click", "button", function(e){
  const buttonId = $(this).prop("name")

  let durationMillis = 0
  let $buttonTimer
  if(buttonId == "dryrun") {
    $buttonTimer = $("#dryRunButtonTimer")
  } else {
    $buttonTimer = $("#runButtonTimer")
  }
  $buttonTimer.text("")
  $buttonTimer.show()
  $("#outputBox").hide()
  //$("#loadingImg").show()

  const data = {
    "config": $("#configTextarea").val(),
    "type": buttonId
  }

  const processId = setInterval(() => {
    durationMillis += 100
    $buttonTimer.text("" + durationMillis);
  }, 100)

  const success = function(result){
    //$("#loadingImg").hide()
    $("#outputBox").show()
    $("#outputArea").text(JSON.stringify(result, null , "\t"))
    $buttonTimer.hide()
    clearInterval(processId)
  }

  const error = function(XMLHttpRequest, status, errorThrown){
    //$("#loadingImg").hide()
    $("#outputBox").show()
    if(status == "timeout"){
      $("#outputArea").text("request timeout")
    }
    if(errorThrown){
      $("#outputArea").text("error: " + errorThrown)
    }
    clearInterval(processId)
  }

  send(data, success, error)
})

const send = function(data, success, error) {
  $.ajax({
    url: "/api",
    data: JSON.stringify(data),
    type: "POST",
    dataType: "json",
    contentType: "application/json",
    cache: false,
    timeout: 30000,
    //processData: false,
    success: success,
    error: error
  })
}

})(jQuery);