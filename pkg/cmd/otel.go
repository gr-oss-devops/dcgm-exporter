package cmd

import (
	"context"
	"time"

	"github.com/NVIDIA/dcgm-exporter/pkg/dcgmexporter"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serviceName = "dcgm-exporter"

func initOtelConn(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func initOtelMeterProvider(ctx context.Context, resource *resource.Resource, conn *grpc.ClientConn, interval time.Duration) (func(context.Context) error, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	// TODO: configurable interval
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(170*time.Millisecond))),
		sdkmetric.WithResource(resource),
	)
	otel.SetMeterProvider(meterProvider)
	return meterProvider.Shutdown, nil
}

func initOtel(ctx context.Context, c *dcgmexporter.Config) (func(context.Context) error, error) {
	conn, err := initOtelConn(c.OtelCollector)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)))
	if err != nil {
		return nil, err
	}

	interval := time.Duration(c.CollectInterval) * time.Millisecond

	shutdown, err := initOtelMeterProvider(context.Background(), res, conn, interval)
	if err != nil {
		return nil, err
	}

	return shutdown, nil
}

func fillOtelMeter(c *dcgmexporter.Config) {
	c.OtelMeter = otel.Meter("dcgm-exporter")
}
