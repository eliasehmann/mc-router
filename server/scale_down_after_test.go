package server

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/swarm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sleeperSignal returns a sleeper and the channel it signals on when it runs.
func sleeperSignal() (SleeperFunc, <-chan struct{}) {
	called := make(chan struct{}, 1)
	return func(context.Context) error {
		called <- struct{}{}
		return nil
	}, called
}

func assertScaledDown(t *testing.T, called <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-called:
	case <-time.After(within):
		t.Fatalf("not scaled down within %s", within)
	}
}

func assertNotScaledDown(t *testing.T, called <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-called:
		t.Fatalf("scaled down within %s", within)
	case <-time.After(within):
	}
}

func TestRouteScaleDownAfterOverridesGlobalDelay(t *testing.T) {
	const globalDelay = 400 * time.Millisecond
	const routeDelay = 20 * time.Millisecond

	for _, test := range []struct {
		name     string
		register func(routes IRoutes, target ScalingTarget, options RouteOptions)
	}{
		{
			name: "CreateMapping",
			register: func(routes IRoutes, target ScalingTarget, options RouteOptions) {
				routes.CreateMapping("mc.example.com", "backend:25565", target, options)
			},
		},
		{
			name: "UpdateMapping",
			register: func(routes IRoutes, target ScalingTarget, options RouteOptions) {
				// scaled down at first, so the update to a routable backend starts the timer
				routes.CreateMapping("mc.example.com", "", target, RouteOptions{Sleeper: options.Sleeper})
				routes.UpdateMapping("mc.example.com", "backend:25565", target, options)
			},
		},
		{
			name: "SetDefaultRoute",
			register: func(routes IRoutes, target ScalingTarget, options RouteOptions) {
				routes.SetDefaultRoute("backend:25565", target, options)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			routes := NewRoutes(t.Context()).WithDownScaler(NewDownScaler(true, globalDelay))
			sleeper, called := sleeperSignal()

			test.register(routes, TestingScalingTarget("backend"), RouteOptions{Sleeper: sleeper, ScaleDownAfter: routeDelay})

			assertScaledDown(t, called, globalDelay/2)
		})
	}
}

func TestRouteWithoutScaleDownAfterUsesGlobalDelay(t *testing.T) {
	const globalDelay = 20 * time.Millisecond
	const routeDelay = 400 * time.Millisecond

	downScaler := NewDownScaler(true, globalDelay)
	routes := NewRoutes(t.Context()).WithDownScaler(downScaler)
	slowSleeper, slowCalled := sleeperSignal()
	sleeper, called := sleeperSignal()

	routes.CreateMapping("slow.example.com", "slow:25565", TestingScalingTarget("slow"), RouteOptions{Sleeper: slowSleeper, ScaleDownAfter: routeDelay})
	routes.CreateMapping("mc.example.com", "backend:25565", TestingScalingTarget("backend"), RouteOptions{Sleeper: sleeper})

	assertScaledDown(t, called, routeDelay/2)
	assertNotScaledDown(t, slowCalled, routeDelay/4)
}

func TestRouteScaleDownAfterIsForgottenWithLastRoute(t *testing.T) {
	const globalDelay = 400 * time.Millisecond
	const routeDelay = 20 * time.Millisecond

	downScaler := NewDownScaler(true, globalDelay)
	routes := NewRoutes(t.Context()).WithDownScaler(downScaler)
	sleeper, called := sleeperSignal()
	target := TestingScalingTarget("backend")
	options := RouteOptions{Sleeper: sleeper, ScaleDownAfter: routeDelay}

	// Two server addresses of one backend, both scaled down for now
	routes.CreateMapping("mc.example.com", "", target, options)
	routes.CreateMapping("alias.example.com", "", target, options)

	// The backend keeps its delay while one of its routes remains
	require.True(t, routes.RemoveMapping("alias.example.com"))
	downScaler.Start(t.Context(), target, routes)
	assertScaledDown(t, called, globalDelay/2)

	// A new route of the same target without an override is back on the global delay
	require.True(t, routes.RemoveMapping("mc.example.com"))
	routes.CreateMapping("mc.example.com", "backend:25565", target, RouteOptions{Sleeper: sleeper})
	assertNotScaledDown(t, called, globalDelay/2)
	assertScaledDown(t, called, globalDelay)
}

func TestKubernetesScaleDownAfterAnnotation(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "duration", value: "45m", want: 45 * time.Minute},
		{name: "unset", value: "", want: 0},
		{name: "invalid falls back to the global delay", value: "soon", want: 0},
		{name: "negative falls back to the global delay", value: "-5m", want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			annotations := map[string]string{AnnotationExternalServerName: "mc.example.com"}
			if test.value != "" {
				annotations[AnnotationAutoScaleDownAfter] = test.value
			}
			service := &core.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "minecraft", Namespace: "default", Annotations: annotations},
				Spec:       core.ServiceSpec{ClusterIP: "10.0.0.5"},
			}
			routesHandler := setupRoutesHandlerMock()
			watcher := &K8sWatcher{autoScaleDown: true, routesHandler: routesHandler}

			watcher.handleAdd(service)

			routesHandler.AssertCalled(t, "CreateMapping", "mc.example.com", "10.0.0.5:25565", NewK8sScalingTarget("default", "minecraft"),
				mock.MatchedBy(func(options RouteOptions) bool { return options.ScaleDownAfter == test.want }))
		})
	}
}

func TestDockerScaleDownAfterLabel(t *testing.T) {
	inspectWith := func(value string) *container.InspectResponse {
		return &container.InspectResponse{
			ContainerJSONBase: &container.ContainerJSONBase{
				ID:    "minecraft",
				Name:  "minecraft",
				State: &container.State{Running: true},
			},
			Config: &container.Config{Labels: map[string]string{
				DockerRouterLabelHost:               "mc.example.com",
				DockerRouterLabelAutoScaleDownAfter: value,
			}},
			NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{
				"minecraft": {IPAddress: "10.0.0.1"},
			}},
		}
	}
	watcher := &dockerWatcherImpl{}

	data, ok := watcher.parseContainerData(inspectWith("12h"))
	require.True(t, ok)
	assert.Equal(t, 12*time.Hour, data.autoScaleDownAfter)

	for _, invalid := range []string{"soon", "0s", "-5m"} {
		t.Run(invalid, func(t *testing.T) {
			_, ok := watcher.parseContainerData(inspectWith(invalid))
			assert.False(t, ok, "an invalid scale-down delay must reject the container")
		})
	}
}

func TestSwarmScaleDownAfterLabel(t *testing.T) {
	serviceWith := func(value string) *swarm.Service {
		return &swarm.Service{Spec: swarm.ServiceSpec{Annotations: swarm.Annotations{Labels: map[string]string{
			DockerRouterLabelHost:               "mc.example.com",
			DockerRouterLabelAutoScaleDownAfter: value,
		}}}}
	}
	watcher := &dockerSwarmWatcherImpl{}

	var data parsedDockerServiceData
	require.True(t, watcher.parseServiceLabels(serviceWith("12h"), &data))
	assert.Equal(t, 12*time.Hour, data.autoScaleDownAfter)

	for _, invalid := range []string{"soon", "0s", "-5m"} {
		t.Run(invalid, func(t *testing.T) {
			assert.False(t, watcher.parseServiceLabels(serviceWith(invalid), &parsedDockerServiceData{}),
				"an invalid scale-down delay must reject the service")
		})
	}
}
