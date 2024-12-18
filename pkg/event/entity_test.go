package event

import (
	"testing"
)

// Mock implementation for meta.AddEntityAttrGroupByName
func AddEntityAttrGroupByName(name string) error {
	// Mock behavior
	return nil
}

func TestAddEntityAttrGroupByName(t *testing.T) {
	// Add your test cases here
	if err := AddEntityAttrGroupByName("test"); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}
