package com.mercari.solution.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProbeService {

    private static final Logger LOG = LoggerFactory.getLogger(ProbeService.class);

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        switch (request.getPathInfo()) {
            case "/ready" -> ready(request, response);
            case "/health" -> health(request, response);
        }
    }

    private static void ready(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        LOG.info("ready");
        response.setStatus(200);
    }

    private static void health(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        LOG.info("health");
        response.setStatus(200);
    }

}
