package com.groww.grpc.kuberesolver;

import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.internal.GrpcUtil;

import java.net.URI;
import java.util.Objects;
import javax.annotation.Nullable;

public class KubernetesNameResolverProvider extends NameResolverProvider {
    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Override
    public String getDefaultScheme() {
        return "kubernetes";
    }

    @Nullable
    @Override
    public KubernetesNameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (targetUri.getScheme().equals(getDefaultScheme())) {
            if (Objects.isNull(targetUri.getPath())) {
                throw new NullPointerException("Must provide target path like kubernetes:///{namespace}/{service}:{port}");
            }
            String[] parts = targetUri.getPath().split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Must be formatted like kubernetes:///{namespace}/{service}:{port}");
            }

            String[] split = parts[0].split("/");
            if (split.length != 3) {
                throw new IllegalArgumentException("Must be formatted like kubernetes:///{namespace}/{service}:{port}");
            }
            try {
                int port = Integer.parseInt(parts[1]);
                return new KubernetesNameResolver(split[1], split[2], port,
                        GrpcUtil.SHARED_CHANNEL_EXECUTOR, GrpcUtil.TIMER_SERVICE);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unable to parse port number", e);
            }
        } else {
            return null;
        }
    }
}
