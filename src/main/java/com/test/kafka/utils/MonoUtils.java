package com.test.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Fuseable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class MonoUtils {

    public static Scheduler DB_QUERY_SCHEDULER = Schedulers.newElastic("dandan-db-query");
    public static Scheduler REDIS_QUERY_SCHEDULER = Schedulers.newElastic("dandan-redis-query");
    public static Scheduler JACKSON_SCHEDULER = Schedulers.newElastic("dandan-jackson-executor");
    private static final Logger log = LoggerFactory.getLogger(MonoUtils.class);

    /**
     * 包裹 sql 查询，并生成 mono
     *
     * @param query
     * @param <T>
     * @return
     */
    public static <T> Mono<T> executeSql(Callable<T> query) {
        return Mono.fromCallable(query)
                .doOnError(err -> log.error("MonoUtils 执行 SQL 出错，错误信息: " + err.getMessage(), err))
                .subscribeOn(DB_QUERY_SCHEDULER);
    }

    public static <T> Mono<T> executeRedis(Callable<T> query) {
        return Mono.fromCallable(query)
                .doOnError(err -> log.error("MonoUtils 执行 Redis 出错，错误信息: " + err.getMessage(), err))
                .subscribeOn(REDIS_QUERY_SCHEDULER);
    }

    public static <T> Mono<T> readJson(Callable<T> callable) {
//        return Mono.fromCallable(callable);
        return Mono.fromCallable(callable)
                .doOnError(err -> log.error("MonoUtils 执行 JSON 解析出错，错误信息: " + err.getMessage(), err))
                .subscribeOn(JACKSON_SCHEDULER);
    }
}
