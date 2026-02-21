package io.rwa.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.config.RwaProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class AdminTokenFilter extends OncePerRequestFilter {

    private final RwaProperties properties;
    private final ObjectMapper objectMapper;

    public AdminTokenFilter(RwaProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
        throws ServletException, IOException {
        String path = request.getRequestURI();
        if (!path.startsWith("/admin/")) {
            filterChain.doFilter(request, response);
            return;
        }

        String configuredToken = properties.getAdminApiToken();
        if (configuredToken == null || configuredToken.isBlank()) {
            unauthorized(response, 500, "ADMIN_API_TOKEN is not configured");
            return;
        }

        String provided = request.getHeader("X-Admin-Token");
        if (!configuredToken.equals(provided)) {
            unauthorized(response, 401, "Invalid admin token");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private void unauthorized(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write(objectMapper.createObjectNode()
            .put("status", status)
            .put("message", message)
            .toString());
    }
}
