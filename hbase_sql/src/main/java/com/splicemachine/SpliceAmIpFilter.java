package com.splicemachine;

import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter;

import javax.servlet.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * Created by jleach on 11/16/16.
 */
public class SpliceAmIpFilter extends AmIpFilter {

    public SpliceAmIpFilter() {
        super();
        System.out.println("SpliceAmIpFilter");
    }

    @Override
    public void init(FilterConfig conf) throws ServletException {
        System.out.println("init -> " + conf.getInitParameterNames());
        super.init(conf);
    }

    @Override
    protected Set<String> getProxyAddresses() throws ServletException {
        System.out.println("getProxyAddresses -> " + super.getProxyAddresses());
        return super.getProxyAddresses();
    }

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        super.doFilter(req, resp, chain);
    }

    @Override
    protected String findRedirectUrl() throws ServletException {
        return super.findRedirectUrl();
    }
}
