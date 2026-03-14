// Package plugin provides a registry and built-in scheduler plugins for
// extending GoSched's scheduling pipeline.
package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// Registry manages scheduler plugins.
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]scheduler.SchedulerPlugin
	order   []string
	logger  *slog.Logger
}

// NewRegistry creates a new plugin Registry.
func NewRegistry(logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{
		plugins: make(map[string]scheduler.SchedulerPlugin),
		order:   make([]string, 0),
		logger:  logger,
	}
}

// Register adds a plugin to the registry.
func (r *Registry) Register(plugin scheduler.SchedulerPlugin) error {
	if plugin == nil {
		return fmt.Errorf("plugin is nil")
	}
	name := plugin.Name()
	if name == "" {
		return fmt.Errorf("plugin name is empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.plugins[name]; exists {
		return fmt.Errorf("plugin %q already registered", name)
	}
	r.plugins[name] = plugin
	r.order = append(r.order, name)
	r.logger.Info("plugin registered", "plugin", name)
	return nil
}

// Unregister removes a plugin from the registry.
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.plugins[name]; !exists {
		return fmt.Errorf("plugin %q not found", name)
	}
	delete(r.plugins, name)
	filtered := make([]string, 0, len(r.order)-1)
	for _, n := range r.order {
		if n != name {
			filtered = append(filtered, n)
		}
	}
	r.order = filtered
	return nil
}

// Get returns a plugin by name.
func (r *Registry) Get(name string) (scheduler.SchedulerPlugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.plugins[name]
	return p, ok
}

// All returns all registered plugins in registration order.
func (r *Registry) All() []scheduler.SchedulerPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]scheduler.SchedulerPlugin, 0, len(r.order))
	for _, name := range r.order {
		if p, ok := r.plugins[name]; ok {
			result = append(result, p)
		}
	}
	return result
}

// RunFilters executes the filter phase across all plugins in order.
func (r *Registry) RunFilters(ctx context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) ([]*scheduler.NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	current := nodes
	for _, name := range r.order {
		p := r.plugins[name]
		filtered, err := p.Filter(ctx, job, current)
		if err != nil {
			r.logger.Error("plugin filter failed", "plugin", name, "error", err)
			return nil, fmt.Errorf("plugin %q filter: %w", name, err)
		}
		current = filtered
	}
	return current, nil
}

// RunScoring executes the score phase across all plugins and aggregates scores.
func (r *Registry) RunScoring(ctx context.Context, job *scheduler.Job, nodes []*scheduler.NodeInfo) (map[string]int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	aggregated := make(map[string]int, len(nodes))
	for _, name := range r.order {
		p := r.plugins[name]
		scores, err := p.Score(ctx, job, nodes)
		if err != nil {
			r.logger.Error("plugin scoring failed", "plugin", name, "error", err)
			return nil, fmt.Errorf("plugin %q score: %w", name, err)
		}
		for nodeID, score := range scores {
			aggregated[nodeID] += score
		}
	}
	return aggregated, nil
}
