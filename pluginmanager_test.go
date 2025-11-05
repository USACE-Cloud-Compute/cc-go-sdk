package cc

import (
	"reflect"
	"testing"
)

func TestSubstituteMapVariablesEnvOnly(t *testing.T) {
	//pm := PluginManager{}
	pm, err := InitPluginManager()
	if err != nil {
		t.Fatal(err)
	}

	payloadAttrs := map[string]any{
		"val1":   1,
		"val2":   "two",
		"val3":   "this is a {ENV::TESTV3}",
		"val4":   "this is {ATTR::testv4} ok?",
		"testv4": "LOREM IPSUM",
		"val5": map[string]any{
			"v5test1": "test 1 of val5",
			"v5test2": "this is a test of {ENV::V5TEST1}",
			"v5test3": []string{
				"v5t3-1{ENV::TESTV3}-ok",
				"v5t3-2{ENV::TESTV3}-ok",
				"v5t3-3{ENV::TESTV3}-ok",
				"v5t3-4{ENV::TESTV3}-ok",
			},
		},
	}
	pm.Attributes = payloadAttrs

	pm.substituteMapVariables(payloadAttrs, false)

	expectedResult := map[string]any{
		"val1":   1,
		"val2":   "two",
		"val3":   "this is a 98765432",
		"val4":   "this is {ATTR::testv4} ok?",
		"testv4": "LOREM IPSUM",
		"val5": map[string]any{
			"v5test1": "test 1 of val5",
			"v5test2": "this is a test of this is a test",
			"v5test3": []string{
				"v5t3-198765432-ok",
				"v5t3-298765432-ok",
				"v5t3-398765432-ok",
				"v5t3-498765432-ok",
			},
		},
	}

	if !reflect.DeepEqual(map[string]any(pm.Attributes), expectedResult) {
		t.Fatalf("expected: %v found %v", expectedResult, pm.Attributes)
	}
}

func TestSubstituteMapVariables(t *testing.T) {
	//pm := PluginManager{}
	pm, err := InitPluginManager()
	if err != nil {
		t.Fatal(err)
	}

	payloadAttrs := map[string]any{
		"val1":   1,
		"val2":   "two",
		"val3":   "this is a {ENV::TESTV3}",
		"val4":   "this is {ATTR::testv4} ok?",
		"testv4": "LOREM IPSUM",
		"val5": map[string]any{
			"v5test1": "test 1 of val5",
			"v5test2": "this is a test of {ENV::V5TEST1}",
			"v5test3": []string{
				"v5t3-1{ENV::TESTV3}-ok",
				"v5t3-2{ENV::TESTV3}-ok",
				"v5t3-3{ENV::TESTV3}-ok",
				"v5t3-4{ENV::TESTV3}-ok",
			},
		},
	}
	pm.Attributes = payloadAttrs
	pm.Actions = []Action{
		{
			IOManager: IOManager{
				Attributes: payloadAttrs,
			},
		},
	}

	pm.substituteMapVariables(pm.Actions[0].Attributes, true)

	expectedResult := map[string]any{
		"val1":   1,
		"val2":   "two",
		"val3":   "this is a 98765432",
		"val4":   "this is LOREM IPSUM ok?",
		"testv4": "LOREM IPSUM",
		"val5": map[string]any{
			"v5test1": "test 1 of val5",
			"v5test2": "this is a test of this is a test",
			"v5test3": []string{
				"v5t3-198765432-ok",
				"v5t3-298765432-ok",
				"v5t3-398765432-ok",
				"v5t3-498765432-ok",
			},
		},
	}

	if !reflect.DeepEqual(map[string]any(pm.Attributes), expectedResult) {
		t.Fatalf("expected: %v found %v", expectedResult, pm.Attributes)
	}
}
