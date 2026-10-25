package com.nageoffer.shortlink.gateway.predicates;

import org.springframework.cloud.gateway.handler.predicate.AbstractRoutePredicateFactory;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.server.ServerWebExchange;

import java.util.function.Predicate;

@Component
public class ShortCodeRoutePredicateFactory extends AbstractRoutePredicateFactory<ShortCodeRoutePredicateFactory.Config> {

    public ShortCodeRoutePredicateFactory() {
        super(Config.class);
    }

    @Override
    public Predicate<ServerWebExchange> apply(Config config) {
        return exchange -> {
            String path = exchange.getRequest().getURI().getPath();
            // 必须是 /xxxxxx 且 x 长度为6，仅含 [a-zA-Z0-9]
            return (path.matches("^/[a-zA-Z0-9]{6}$") || path.matches("/page/notfound"));
        };
    }

    @Validated
    public static class Config {
        // 可扩展参数，当前不需要
    }

}