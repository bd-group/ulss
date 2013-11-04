/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.handler;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 *
 * @author mwm
 */
public class HelloHandler extends AbstractHandler {

    final String _greeting;

    final String _body;

    public HelloHandler() {
        _greeting = "Hello World";
        _body = null;
    }

    public HelloHandler(String greeting) {
        _greeting = greeting;
        _body = null;
    }

    public HelloHandler(String greeting, String body) {
        _greeting = greeting;
        _body = body;
    }

    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        System.out.println("get request:" + request.getMethod());
        Request baseRequest = (request instanceof Request) ? (Request) request : HttpConnection.getCurrentConnection().getRequest();
        baseRequest.setHandled(true);
        
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        response.getWriter().println("<h1>" + _greeting + "</h1>");
        if (_body != null) response.getWriter().println(_body);
    }
}
