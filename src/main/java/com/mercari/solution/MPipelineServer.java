package com.mercari.solution;

import com.mercari.solution.api.PipelineService;
import com.mercari.solution.api.ProbeService;
import com.mercari.solution.api.SchemaService;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.*;


public class MPipelineServer extends HttpServlet {

    //private ServerLogHandler serverLogHandler;

    @Override
    public void init() {
        //serverLogHandler = createLogHandler();
    }

    /*
    private static ServerLogHandler createLogHandler() {
        final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        final ServerLogHandler serverLogHandler = ServerLogHandler.of(Level.ALL);
        rootLogger.addHandler(serverLogHandler);
        rootLogger.setLevel(Level.ALL);
        return serverLogHandler;
    }
     */

    @Override
    protected void doGet(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        switch (request.getServletPath()) {
            case "/probe" -> ProbeService.serve(request, response);
            case "/api/schema" -> SchemaService.serve(request, response);
            case "/api/pipeline" -> PipelineService.serve(request, response);
            default -> {}
        }
    }

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        switch (request.getServletPath()) {
            case "/probe" -> ProbeService.serve(request, response);
            case "/api/schema" -> SchemaService.serve(request, response);
            case "/api/pipeline" -> PipelineService.serve(request, response);
            default -> {}
        }
    }

}
