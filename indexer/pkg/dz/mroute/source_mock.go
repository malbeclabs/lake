package mroute

import "context"

// MockSource is a Source implementation for testing.
type MockSource struct {
	Dumps    []*Dump
	FetchErr error
	Closed   bool
}

// NewMockSource creates a MockSource preloaded with the given dumps.
func NewMockSource(dumps ...*Dump) *MockSource {
	return &MockSource{Dumps: dumps}
}

// FetchLatest returns the configured dumps or error.
func (m *MockSource) FetchLatest(ctx context.Context) ([]*Dump, error) {
	if m.FetchErr != nil {
		return nil, m.FetchErr
	}
	return m.Dumps, nil
}

// Close marks the source as closed.
func (m *MockSource) Close() error {
	m.Closed = true
	return nil
}
