<%@ page language="java" contentType="text/html; charset=UTF-8"	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html style="height:100%;">
<head>
  <meta charset="UTF-8">
  <title>Mercari Pipeline Playground</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-9ndCyUaIbzAi2FUVXJi0CjmCapSmO7SnpJef0486qhLnuZ2cdeRhO02iuK6FUUVM" crossorigin="anonymous">
  <link rel="stylesheet" href="/css/layout.css?id=1">
</head>
<body style="height: 100%;">
  <header id="header" class="navbar navbar-fixed-top navbar-inverse" role="navigation" style="height: 80px;">
    <div id="pageControl" class="" style="margin-left:0px;margin-right:0px; repeat-x scroll left top rgba(0, 0, 0, 0);">
      <div id="groupHeader" style="padding:0 15px;">
        <div style="float:left;margin: 3px 15px 8px 0;padding:0px;" class="">
          <p style="#444444;color: #aaaaaa;border:none;font-size: 30px">Mercari Pipeline</p>
        </div>
      </div>
      <div style="clear: both;"></div>
    </div>
    <div id="buttons" style="padding-right: 20px">
      <button id="dryRunButton" type="button" name="dryrun" class="btn btn-secondary" style="width: 150px">Dry Run<span id="dryRunButtonTimer" style="display:none; margin-left: 10px;"></span></button>
      <button id="runButton" type="button" name="run" class="btn btn-primary" style="width: 150px">Run<span id="runButtonTimer" style="display:none; margin-left: 10px;"></span></button>
      <button id="launchButton" type="button" name="launch" class="btn btn-warning" style="width: 150px">Launch<span id="launchButtonTimer" style="display:none; margin-left: 10px;"></span></button>
    </div>
  </header>
  <main style="width:none; padding: 0px 20px; height: 100%;">
    <div id="input" style="height: 50%;">
      <div class="form-floating" style="height: 100%;">
        <textarea id="configTextarea" class="form-control" placeholder="write config here" style="height: 100%"></textarea>
        <label for="configTextarea">Pipeline Config</label>
      </div>
    </div>
    <div id="output" style="height: 50%;">
      <div id="outputBox" class="form-floating" style="height: 100%;">
        <textarea id="outputArea" class="form-control" placeholder="write config here" style="height: 100%" readonly></textarea>
        <label for="outputArea">Pipeline Result</label>
      </div>
      <div id="loadingImg" style="display:none;margin: 30px;" />
    </div>
  </main>

  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js" integrity="sha384-geWF76RCwLtnZ8qwWowPQNguL3RmwHVBC9FhGdlKrxdiJJigb/j/68SIy3Te4Bkz" crossorigin="anonymous"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/6.65.7/codemirror.min.js"></script>
  <script charset="utf-8" src="/js/base.js?id=20250325" type="text/javascript"></script>
  <script>

  </script>
</body>
</html>