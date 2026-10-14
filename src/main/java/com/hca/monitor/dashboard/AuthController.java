package com.hca.monitor.dashboard;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);
    private static final int MAX_FAILURES = 5;
    private static final Duration LOCKOUT = Duration.ofMinutes(5);

    public record Credentials(String username, String password) {}

    private record Failures(int count, Instant lockedUntil) {}

    private final AuthenticationManager auth;
    private final SecurityContextRepository contexts;
    private final Map<String, Failures> failures = new ConcurrentHashMap<>();

    public AuthController(AuthenticationManager auth, SecurityContextRepository contexts) {
        this.auth = auth;
        this.contexts = contexts;
    }

    @PostMapping("/api/login")
    public ResponseEntity<?> login(@RequestBody Credentials body, HttpServletRequest req, HttpServletResponse res) {
        String client = req.getRemoteAddr();
        Failures f = failures.get(client);
        if (f != null && f.lockedUntil() != null && f.lockedUntil().isAfter(Instant.now())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", "Too many wrong passwords. Try again in a few minutes."));
        }
        try {
            Authentication a = auth.authenticate(UsernamePasswordAuthenticationToken.unauthenticated(
                    body.username() == null ? "" : body.username().trim(), body.password() == null ? "" : body.password()));
            HttpSession old = req.getSession(false);
            if (old != null) old.invalidate();                         // new session id on sign-in
            SecurityContext ctx = SecurityContextHolder.createEmptyContext();
            ctx.setAuthentication(a);
            SecurityContextHolder.setContext(ctx);
            contexts.saveContext(ctx, req, res);
            failures.remove(client);
            log.info("Dashboard sign-in from {}", client);
            return ResponseEntity.ok(Map.of("user", a.getName()));
        } catch (AuthenticationException e) {
            int n = f == null ? 1 : f.count() + 1;
            failures.put(client, new Failures(n, n >= MAX_FAILURES ? Instant.now().plus(LOCKOUT) : null));
            log.warn("Dashboard sign-in failed from {} ({} in a row)", client, n);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Wrong username or password."));
        }
    }

    @PostMapping("/api/logout")
    public ResponseEntity<?> logout(HttpServletRequest req) {
        HttpSession s = req.getSession(false);
        if (s != null) s.invalidate();
        SecurityContextHolder.clearContext();
        return ResponseEntity.noContent().build();
    }
}
