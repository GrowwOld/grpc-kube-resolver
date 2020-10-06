package com.groww.grpc.kuberesolver;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.internal.SharedResourceHolder;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KubernetesNameResolver extends NameResolver {
    private final KubernetesClient kubernetesClient;
    private Listener listener;
    private final String namespace;
    private final String serviceName;
    private final int port;
    private final SharedResourceHolder.Resource<Executor> executorResource;
    private Executor executor;
    private final SharedResourceHolder.Resource<ScheduledExecutorService> timerServiceResource;
    private ScheduledExecutorService scheduledExecutorService;
    private volatile boolean refreshing = false;
    private volatile boolean watching = false;

    KubernetesNameResolver(String namespace, String serviceName,
                           int port, SharedResourceHolder.Resource<Executor> executorResource,
                           SharedResourceHolder.Resource<ScheduledExecutorService> timerServiceResource) {
        this.serviceName = serviceName;
        this.timerServiceResource = timerServiceResource;
        this.kubernetesClient = new DefaultKubernetesClient();
        this.namespace = namespace;
        this.port = port;
        this.executorResource = executorResource;
    }


    @Override
    public String getServiceAuthority() {
        return kubernetesClient.getMasterUrl().getAuthority();
    }

    @Override
    public void start(Listener listener) {
        this.executor = SharedResourceHolder.get(this.executorResource);
        this.scheduledExecutorService = SharedResourceHolder.get(timerServiceResource);
        this.listener = listener;
        refresh();
    }

    public void refresh() {
        if (refreshing) return;
        try {
            log.info("Refreshing endpoints for {}", serviceName);
            refreshing = true;

            Endpoints endpoints = kubernetesClient.endpoints().inNamespace(namespace)
                    .withName(serviceName)
                    .get();

            if (endpoints == null) {
                scheduledExecutorService.schedule(this::refresh, 30, TimeUnit.SECONDS);
                return;
            }

            update(endpoints);
            if (!watching) {
                watching = true;
                executor.execute(this::watch);
            }

        } finally {
            refreshing = false;
        }
    }

    private void update(Endpoints endpoints) {
        List<EquivalentAddressGroup> servers = new ArrayList<>();
        if (endpoints.getSubsets() == null) return;
        endpoints.getSubsets().forEach(subset -> {
            long matchingPorts = subset.getPorts().stream()
                    .filter(p -> p != null && p.getPort() == port)
                    .count();
            if (matchingPorts > 0) {
                subset.getAddresses().stream()
                        .map(address -> new EquivalentAddressGroup(new InetSocketAddress(address.getIp(), port)))
                        .forEach(servers::add);
            }
        });
        log.info("adding new endpoints for {} {}", serviceName, servers);
        listener.onAddresses(servers, Attributes.EMPTY);
    }

    private void watch() {
        try {
            log.info("starting watch for {}", serviceName);
            final CountDownLatch isWatchClosed = new CountDownLatch(1);
            kubernetesClient.endpoints().inNamespace(namespace)
                    .withName(serviceName)
                    .watch(new Watcher<Endpoints>() {
                        @Override
                        public void eventReceived(Action action, Endpoints endpoints) {
                            System.out.println(action + endpoints.toString());
                            switch (action) {
                                case ADDED:
                                case MODIFIED:
                                    update(endpoints);
                                    return;
                                case DELETED:
                                    listener.onAddresses(Collections.emptyList(), Attributes.EMPTY);
                            }
                        }

                        @Override
                        public void onClose(KubernetesClientException e) {
                            log.error("watch stream is closed for {}", serviceName);
                            watching = false;
                            refresh();
                            isWatchClosed.countDown();
                        }
                    });
            isWatchClosed.await();
        } catch (InterruptedException e) {
            log.error("watch stream is interrupted for {}", serviceName);
            watching = false;
            refresh();
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        if (this.executor != null) {
            this.executor = SharedResourceHolder.release(this.executorResource, this.executor);
        }
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService = SharedResourceHolder.release(
                    this.timerServiceResource, this.scheduledExecutorService);
        }
        kubernetesClient.close();
    }
}
