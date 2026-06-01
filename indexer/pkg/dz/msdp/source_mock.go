package msdp

import "context"

// MockSource is a Source implementation for testing.
type MockSource struct {
	Dumps    map[string][]*Dump
	FetchErr error
	Closed   bool
}

// NewMockSource preloads dumps grouped by kind.
func NewMockSource(dumps map[string][]*Dump) *MockSource {
	return &MockSource{Dumps: dumps}
}

func (m *MockSource) FetchLatest(ctx context.Context) (map[string][]*Dump, error) {
	if m.FetchErr != nil {
		return nil, m.FetchErr
	}
	return m.Dumps, nil
}

func (m *MockSource) Close() error {
	m.Closed = true
	return nil
}
