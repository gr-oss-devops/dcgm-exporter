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
)

const serviceName = "dcgm-exporter"

func initOtelMeterProvider(ctx context.Context, resource *resource.Resource, interval time.Duration) (func(context.Context) error, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(interval))),
		sdkmetric.WithResource(resource),
	)
	otel.SetMeterProvider(meterProvider)
	return meterProvider.Shutdown, nil
}

func initOtel(ctx context.Context, c *dcgmexporter.Config) (func(context.Context) error, error) {
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)))
	if err != nil {
		return nil, err
	}

	interval := time.Duration(c.CollectInterval) * time.Millisecond

	shutdown, err := initOtelMeterProvider(context.Background(), res, interval)
	if err != nil {
		return nil, err
	}

	return shutdown, nil
}

func fillOtelMeter(c *dcgmexporter.Config) {
	c.OtelMeter = otel.Meter("dcgm-exporter")
}
