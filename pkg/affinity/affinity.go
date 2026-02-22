// Package affinity implements node affinity, anti-affinity, label selectors,
// and taint toleration for the GoSched scheduling system.
package affinity

import (
	"context"
	"strings"

	"github.com/ilya-shevelev/gosched/pkg/scheduler"
)

// Rule represents an affinity or anti-affinity rule.
type Rule struct {
	// Type is either "affinity" or "anti-affinity".
	Type RuleType
	// Key is the label key to match.
	Key string
	// Operator is the match operator.
	Operator Operator
	// Values are the values to match against.
	Values []string
	// Weight is the importance of this rule (0-100). Only used for preferred rules.
	Weight int
	// Required indicates if this is a hard requirement vs. soft preference.
	Required bool
}

// RuleType distinguishes affinity from anti-affinity.
type RuleType string

// Rule types.
const (
	RuleTypeAffinity     RuleType = "affinity"
	RuleTypeAntiAffinity RuleType = "anti-affinity"
)

// Operator defines how label values are matched.
type Operator string

// Operators.
const (
	OpIn           Operator = "In"
	OpNotIn        Operator = "NotIn"
	OpExists       Operator = "Exists"
	OpDoesNotExist Operator = "DoesNotExist"
	OpGt           Operator = "Gt"
	OpLt           Operator = "Lt"
)

// Toleration allows a job to tolerate a specific taint.
type Toleration struct {
	Key      string
	Operator TolerationOperator
	Value    string
	Effect   scheduler.TaintEffect
}

// TolerationOperator defines how tolerations match taints.
type TolerationOperator string

// Toleration operators.
const (
	TolerationOpEqual  TolerationOperator = "Equal"
	TolerationOpExists TolerationOperator = "Exists"
)

// Matcher evaluates affinity rules and tolerations against nodes.
type Matcher struct{}

// NewMatcher creates a new affinity Matcher.
func NewMatcher() *Matcher {
	return &Matcher{}
}

// FilterByRules filters nodes based on the given affinity/anti-affinity rules.
// Required rules are hard constraints; nodes that violate them are removed.
func (m *Matcher) FilterByRules(_ context.Context, nodes []*scheduler.NodeInfo, rules []Rule) []*scheduler.NodeInfo {
	if len(rules) == 0 {
		return nodes
	}

	result := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if m.satisfiesRequired(node, rules) {
			result = append(result, node)
		}
	}
	return result
}

// ScoreByRules scores nodes based on preferred (soft) affinity rules.
// Returns scores from 0-100 per node.
func (m *Matcher) ScoreByRules(_ context.Context, nodes []*scheduler.NodeInfo, rules []Rule) map[string]int {
	scores := make(map[string]int, len(nodes))
	if len(rules) == 0 {
		return scores
	}

	totalWeight := 0
	for _, rule := range rules {
		if !rule.Required {
			totalWeight += rule.Weight
		}
	}
	if totalWeight == 0 {
		return scores
	}

	for _, node := range nodes {
		score := 0
		for _, rule := range rules {
			if rule.Required {
				continue
			}
			matches := matchRule(node.Labels, rule)
			if (rule.Type == RuleTypeAffinity && matches) ||
				(rule.Type == RuleTypeAntiAffinity && !matches) {
				score += rule.Weight
			}
		}
		// Normalize to 0-100.
		scores[node.ID] = score * 100 / totalWeight
	}
	return scores
}

// FilterByTolerations filters nodes based on tolerations. A node is excluded
// if it has a taint with NoSchedule effect that is not tolerated.
func (m *Matcher) FilterByTolerations(_ context.Context, nodes []*scheduler.NodeInfo, tolerations []Toleration) []*scheduler.NodeInfo {
	result := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if m.toleratesTaints(node.Taints, tolerations) {
			result = append(result, node)
		}
	}
	return result
}

// FilterByNodeSelector filters nodes using exact label matching.
func (m *Matcher) FilterByNodeSelector(_ context.Context, nodes []*scheduler.NodeInfo, selector map[string]string) []*scheduler.NodeInfo {
	if len(selector) == 0 {
		return nodes
	}

	result := make([]*scheduler.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if matchesSelector(node.Labels, selector) {
			result = append(result, node)
		}
	}
	return result
}

// satisfiesRequired checks if a node satisfies all required rules.
func (m *Matcher) satisfiesRequired(node *scheduler.NodeInfo, rules []Rule) bool {
	for _, rule := range rules {
		if !rule.Required {
			continue
		}
		matches := matchRule(node.Labels, rule)
		if rule.Type == RuleTypeAffinity && !matches {
			return false
		}
		if rule.Type == RuleTypeAntiAffinity && matches {
			return false
		}
	}
	return true
}

// toleratesTaints returns true if all NoSchedule taints are tolerated.
func (m *Matcher) toleratesTaints(taints []scheduler.Taint, tolerations []Toleration) bool {
	for _, taint := range taints {
		if taint.Effect != scheduler.TaintEffectNoSchedule {
			continue
		}
		if !isTolerated(taint, tolerations) {
			return false
		}
	}
	return true
}

// matchRule evaluates whether a label set matches a single rule.
func matchRule(labels map[string]string, rule Rule) bool {
	value, exists := labels[rule.Key]
	switch rule.Operator {
	case OpIn:
		if !exists {
			return false
		}
		for _, v := range rule.Values {
			if v == value {
				return true
			}
		}
		return false
	case OpNotIn:
		if !exists {
			return true
		}
		for _, v := range rule.Values {
			if v == value {
				return false
			}
		}
		return true
	case OpExists:
		return exists
	case OpDoesNotExist:
		return !exists
	case OpGt:
		if !exists || len(rule.Values) == 0 {
			return false
		}
		return strings.Compare(value, rule.Values[0]) > 0
	case OpLt:
		if !exists || len(rule.Values) == 0 {
			return false
		}
		return strings.Compare(value, rule.Values[0]) < 0
	default:
		return false
	}
}

// matchesSelector checks if labels contain all selector key-value pairs.
func matchesSelector(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// isTolerated checks if a taint is matched by any toleration.
func isTolerated(taint scheduler.Taint, tolerations []Toleration) bool {
	for _, tol := range tolerations {
		if tol.Operator == TolerationOpExists {
			if tol.Key == "" || tol.Key == taint.Key {
				if tol.Effect == "" || tol.Effect == taint.Effect {
					return true
				}
			}
			continue
		}
		// TolerationOpEqual
		if tol.Key == taint.Key && tol.Value == taint.Value {
			if tol.Effect == "" || tol.Effect == taint.Effect {
				return true
			}
		}
	}
	return false
}
