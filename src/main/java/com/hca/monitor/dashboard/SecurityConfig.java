package com.hca.monitor.dashboard;

import jakarta.servlet.DispatcherType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;

import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * One shared dashboard account, set with DASHBOARD_USER / DASHBOARD_PASSWORD. If no password is set,
 * a random one is generated at startup and written to the log, so the dashboard is never open by accident.
 * The API is JSON-only behind a session cookie: unauthenticated calls get 401 and the page shows its sign-in form.
 */
@Configuration
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    UserDetailsService dashboardUser(@Value("${monitor.dashboard.username:pipeline}") String username,
                                     @Value("${monitor.dashboard.password:}") String password,
                                     PasswordEncoder encoder) {
        String pw = password;
        if (pw == null || pw.isBlank()) {
            byte[] b = new byte[9];
            new SecureRandom().nextBytes(b);
            pw = HexFormat.of().formatHex(b);
            log.warn("DASHBOARD_PASSWORD is not set - generated one for this run. Sign in as '{}' with password: {}", username, pw);
        }
        return new InMemoryUserDetailsManager(User.withUsername(username).password(encoder.encode(pw)).roles("VIEWER").build());
    }

    @Bean
    AuthenticationManager authenticationManager(UserDetailsService users, PasswordEncoder encoder) {
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider(users);
        provider.setPasswordEncoder(encoder);
        return new ProviderManager(provider);
    }

    @Bean
    SecurityContextRepository securityContextRepository() {
        return new HttpSessionSecurityContextRepository();
    }

    @Bean
    SecurityFilterChain api(HttpSecurity http, SecurityContextRepository contexts) throws Exception {
        return http
                // JSON API on the same origin as the page, session cookie is SameSite=Lax: no form posts to forge.
                .csrf(c -> c.disable())
                .securityContext(c -> c.securityContextRepository(contexts))
                .authorizeHttpRequests(a -> a
                        // SSE completions and error pages are re-dispatches of an already-checked request
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/api/login", "/api/logout").permitAll()
                        .anyRequest().authenticated())
                .exceptionHandling(e -> e.authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                .formLogin(f -> f.disable())
                .httpBasic(b -> b.disable())
                .logout(l -> l.disable())
                .build();
    }
}
