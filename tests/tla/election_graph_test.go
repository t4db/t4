package tla_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

type lockSnapshot struct {
	owner int
	term  uint64
	fence election.Fence
}

type lockCall struct {
	operation int
	node      int
	floor     uint64
	fence     election.Fence
	expired   bool
}

type graphState struct {
	lock lockSnapshot
	call lockCall
	won  bool
}

type electionGraph struct {
	initial string
	states  map[string]graphState
	edges   map[string][]string
}

// The pinned TLC DOT format stores graph edges and a label per state. Decode
// just the three scalar/tuple fields declared by Election.tla.
var (
	stateLine = regexp.MustCompile(`^(-?[0-9]+) \[label="([^"]+)"`)
	edgeLine  = regexp.MustCompile(`^(-?[0-9]+) -> (-?[0-9]+) \[`)
	lockField = regexp.MustCompile(`lock = <<([^>]+)>>`)
	callField = regexp.MustCompile(`call = <<([^>]+)>>`)
	wonField  = regexp.MustCompile(`won = (TRUE|FALSE)`)
)

func decodeState(label string) (graphState, error) {
	lock := lockField.FindStringSubmatch(label)
	call := callField.FindStringSubmatch(label)
	won := wonField.FindStringSubmatch(label)
	if lock == nil || call == nil || won == nil {
		return graphState{}, fmt.Errorf("incomplete TLC state: %s", label)
	}
	var state graphState
	l, c := &state.lock, &state.call
	if _, err := fmt.Sscanf(lock[1], "%d, %d, %d, %d, %d",
		&l.owner, &l.term, &l.fence.Rev, &l.fence.Seq, &l.fence.Term); err != nil {
		return state, fmt.Errorf("decode lock %q: %w", lock[1], err)
	}
	var expired string
	if _, err := fmt.Sscanf(call[1], "%d, %d, %d, %d, %d, %d, %s",
		&c.operation, &c.node, &c.floor, &c.fence.Rev, &c.fence.Seq, &c.fence.Term, &expired); err != nil {
		return state, fmt.Errorf("decode call %q: %w", call[1], err)
	}
	if expired != "TRUE" && expired != "FALSE" {
		return state, fmt.Errorf("unknown expiration value %q", expired)
	}
	c.expired = expired == "TRUE"
	state.won = won[1] == "TRUE"
	return state, nil
}

func readElectionGraph(path string) (electionGraph, error) {
	graph := electionGraph{states: make(map[string]graphState), edges: make(map[string][]string)}
	file, err := os.Open(path)
	if err != nil {
		return graph, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	closed := false
	for scanner.Scan() {
		line := scanner.Text()
		if match := edgeLine.FindStringSubmatch(line); match != nil {
			graph.edges[match[1]] = append(graph.edges[match[1]], match[2])
		} else if match := stateLine.FindStringSubmatch(line); match != nil && strings.Contains(match[2], "lock = ") {
			state, err := decodeState(match[2])
			if err != nil {
				return graph, err
			}
			if state.call.operation == 0 {
				if graph.initial != "" || state != (graphState{}) {
					return graph, fmt.Errorf("unexpected initial state: %+v", state)
				}
				graph.initial = match[1]
			}
			graph.states[match[1]] = state
		} else if line == "}" {
			closed = true
		}
	}
	if err := scanner.Err(); err != nil {
		return graph, err
	}
	if graph.initial == "" || len(graph.edges) == 0 || !closed {
		return graph, fmt.Errorf("missing initial state, edges, or end of TLC graph")
	}
	for source, targets := range graph.edges {
		if _, ok := graph.states[source]; !ok {
			return graph, fmt.Errorf("unknown edge source %s", source)
		}
		for _, target := range targets {
			if _, ok := graph.states[target]; !ok || target == graph.initial {
				return graph, fmt.Errorf("invalid edge target %s", target)
			}
		}
	}
	return graph, nil
}

func assertSnapshot(rec *election.LockRecord, want lockSnapshot) error {
	if want.owner == 0 {
		if rec != nil {
			return fmt.Errorf("expected no lock, got %+v", rec)
		}
		return nil
	}
	if rec == nil || rec.NodeID != strconv.Itoa(want.owner) ||
		rec.Term != want.term || rec.Fence() != want.fence {
		return fmt.Errorf("lock = %+v, want owner %d, term %d, fence %+v",
			rec, want.owner, want.term, want.fence)
	}
	return nil
}

func replayElection(t *testing.T, path []graphState) {
	t.Helper()
	ctx := context.Background()
	store := object.NewMem()
	for i, next := range path {
		call := next.call
		node := strconv.Itoa(call.node)
		lock := election.NewLock(store, node, node)
		var rec *election.LockRecord
		var won bool
		var err error
		switch call.operation {
		case 1:
			rec, won, err = lock.TryAcquire(ctx, call.floor, call.fence)
		case 2:
			// The takeover caller owns the expiration decision.
			allow := func(*election.LockRecord) bool { return call.expired }
			rec, won, err = lock.TakeOver(ctx, call.floor, call.fence, allow)
		default:
			t.Fatalf("unknown election operation: %+v", call)
		}
		if err != nil || won != next.won {
			t.Fatalf("step %d: won=%t err=%v, want won=%t; trace=%+v", i, won, err, next.won, path)
		}
		if err := assertSnapshot(rec, next.lock); err != nil {
			t.Fatalf("step %d returned %v; trace=%+v", i, err, path)
		}
		stored, err := lock.Read(ctx)
		if err != nil {
			t.Fatalf("step %d reading lock: %v", i, err)
		}
		if err := assertSnapshot(stored, next.lock); err != nil {
			t.Fatalf("step %d stored %v; trace=%+v", i, err, path)
		}
	}
}

func TestElectionTLCGraph(t *testing.T) {
	path := os.Getenv("T4_TLC_ELECTION_GRAPH")
	if path == "" {
		t.Skip("run make tla to check the fresh TLC graph")
	}
	graph, err := readElectionGraph(path)
	if err != nil {
		t.Fatal(err)
	}

	// Breadth-first parents give one shortest prefix for every source state.
	parents := map[string]string{graph.initial: ""}
	queue := []string{graph.initial}
	for i := 0; i < len(queue); i++ {
		source := queue[i]
		for _, target := range graph.edges[source] {
			if _, seen := parents[target]; !seen {
				parents[target] = source
				queue = append(queue, target)
			}
		}
	}
	if len(queue) != len(graph.states) {
		t.Fatalf("reached %d of %d graph states", len(queue), len(graph.states))
	}

	edges := 0
	for _, source := range queue {
		var prefix []graphState
		for step := source; step != graph.initial; step = parents[step] {
			prefix = append(prefix, graph.states[step])
		}
		for i, j := 0, len(prefix)-1; i < j; i, j = i+1, j-1 {
			prefix[i], prefix[j] = prefix[j], prefix[i]
		}
		for _, target := range graph.edges[source] {
			replayElection(t, append(prefix, graph.states[target]))
			edges++
		}
	}
	t.Logf("replayed %d election transitions from %d TLC states", edges, len(queue))
}
