package io.github.saneea.scalastatuschecker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class App {

    public sealed interface Response {
        record Success(String applicationStatus, String applicationId) implements Response {
        }

        record RetryAfter(Duration delay) implements Response {
        }

        record Failure(Throwable ex) implements Response {
        }
    }

    public sealed interface ApplicationStatusResponse {
        record Failure(Duration lastRequestTime, int retriesCount) implements ApplicationStatusResponse {
        }

        record Success(String id, String status) implements ApplicationStatusResponse {
        }
    }

    public interface Handler {
        ApplicationStatusResponse performOperation(String id);

        void shutdown();
    }

    public interface Client {
        //блокирующий вызов сервиса 1 для получения статуса заявки
        Response getApplicationStatus1(String id);

        //блокирующий вызов сервиса 2 для получения статуса заявки
        Response getApplicationStatus2(String id);

    }

    public static class HandlerImpl implements Handler {

        private final List<Function<String, Response>> statusCheckersFuncs;
        private final ExecutorService executorService;
        private final long performOperationTimeoutMillis;

        record MethodResponse(int methodId, Response response) {
        }

        public HandlerImpl(Client client, long performOperationTimeoutMillis, int threadsCount) {
            this.statusCheckersFuncs = List.of(
                client::getApplicationStatus1,
                client::getApplicationStatus2
            );

            this.performOperationTimeoutMillis = performOperationTimeoutMillis;

            this.executorService = Executors.newFixedThreadPool(threadsCount);
        }

        @Override
        public ApplicationStatusResponse performOperation(String id) {

            long startTime = System.currentTimeMillis();

            BlockingQueue<MethodResponse> methodResponseQueue = new ArrayBlockingQueue<>(statusCheckersFuncs.size());

            List<Future<?>> futures = new ArrayList<>(statusCheckersFuncs.size());
            for (int i = 0; i < statusCheckersFuncs.size(); i++) {
                futures.add(null);
            }


            AtomicInteger retriesCount = new AtomicInteger();

            Consumer<Integer> submitMethodCallTask = methodId -> {
                Function<String, Response> statusCheckersFunc = statusCheckersFuncs.get(methodId);

                Runnable r = () -> {
                    retriesCount.incrementAndGet();
                    Response response = statusCheckersFunc.apply(id);
                    methodResponseQueue.add(new MethodResponse(methodId, response));
                };

                Future<?> future = executorService.submit(r);

                futures.set(methodId, future);
            };

            for (int methodId = 0; methodId < statusCheckersFuncs.size(); methodId++) {
                submitMethodCallTask.accept(methodId);
            }

            while (true) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                long timeLeft = Math.max(performOperationTimeoutMillis - elapsedTime, 0);

                if (timeLeft == 0) {
                    break;
                }

                MethodResponse methodResponse;
                try {
                    methodResponse = methodResponseQueue.poll(timeLeft, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }

                if (methodResponse == null) {
                    break;
                }

                Response response = methodResponse.response();
                if (response instanceof Response.Success success) {
                    futures.forEach(future -> future.cancel(true));
                    return new ApplicationStatusResponse.Success(success.applicationId(), success.applicationStatus());
                } else {
                    submitMethodCallTask.accept(methodResponse.methodId());
                }
            }

            futures.forEach(future -> future.cancel(true));
            return new ApplicationStatusResponse.Failure(null, retriesCount.get());
        }

        @Override
        public void shutdown() {
            executorService.shutdown();
        }
    }

    record WaitAndResponse(long sleep, boolean success) {
    }

    public static void main(String[] args) {

        BlockingQueue<WaitAndResponse> responses1 = new LinkedBlockingQueue<>();
        BlockingQueue<WaitAndResponse> responses2 = new LinkedBlockingQueue<>();

        responses1.add(new WaitAndResponse(1000, false));
        responses2.add(new WaitAndResponse(30000, false));

        responses1.add(new WaitAndResponse(2000, true));

        Client client = new Client() {
            @Override
            public Response getApplicationStatus1(String id) {
                try {
                    WaitAndResponse waitAndResponse = responses1.take();
                    Thread.sleep(waitAndResponse.sleep());

                    return waitAndResponse.success()
                        ? new Response.Success("status 1", id)
                        : new Response.Failure(null);
                } catch (InterruptedException e) {
                    return new Response.Failure(e);
                }
            }

            @Override
            public Response getApplicationStatus2(String id) {
                try {
                    WaitAndResponse waitAndResponse = responses2.take();
                    Thread.sleep(waitAndResponse.sleep());

                    return waitAndResponse.success()
                        ? new Response.Success("status 2", id)
                        : new Response.Failure(null);
                } catch (InterruptedException e) {
                    return new Response.Failure(e);
                }
            }
        };

        Handler handler = new HandlerImpl(client, 5000, 8);
        try {
            ApplicationStatusResponse statusResponse = handler.performOperation("my id");
            System.out.println(statusResponse);
        } finally {
            handler.shutdown();
        }

    }

}
