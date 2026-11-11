package metadata

import (
	"encoding/json"
	"testing"

	"github.com/oekit/registry"
)

func TestInstance_Addr(t *testing.T) {
	inst := registry.Instance{IP: "10.0.0.1", Port: 8080}
	if inst.Addr() != "10.0.0.1:8080" {
		t.Errorf("expected '10.0.0.1:8080', got %q", inst.Addr())
	}
}

func TestInstance_Addr_SmallPort(t *testing.T) {
	inst := registry.Instance{IP: "127.0.0.1", Port: 80}
	if inst.Addr() != "127.0.0.1:80" {
		t.Errorf("expected '127.0.0.1:80', got %q", inst.Addr())
	}
}

func TestInstance_Addr_LargePort(t *testing.T) {
	inst := registry.Instance{IP: "192.168.1.1", Port: 65535}
	if inst.Addr() != "192.168.1.1:65535" {
		t.Errorf("expected '192.168.1.1:65535', got %q", inst.Addr())
	}
}

func TestNewServiceBasic_Defaults(t *testing.T) {
	sb := NewServiceBasic()
	if sb.ConnTimeout != 1000 {
		t.Errorf("expected ConnTimeout 1000, got %d", sb.ConnTimeout)
	}
	if sb.ReadTimeout != 5000 {
		t.Errorf("expected ReadTimeout 5000, got %d", sb.ReadTimeout)
	}
	if sb.WriteTimeout != 5000 {
		t.Errorf("expected WriteTimeout 5000, got %d", sb.WriteTimeout)
	}
	if sb.Retry != 3 {
		t.Errorf("expected Retry 3, got %d", sb.Retry)
	}
	if sb.Lb != LbRoundRobin {
		t.Errorf("expected LbRoundRobin, got %q", sb.Lb)
	}
}

func TestServiceName_Fields(t *testing.T) {
	sn := ServiceName{
		FullName: "cn.infra.ops.myapp.http",
		Region:   "cn",
		Biz1:     "infra",
		Biz2:     "ops",
		Name:     "myapp",
		Protocol: "http",
	}
	if sn.FullName != "cn.infra.ops.myapp.http" {
		t.Errorf("unexpected FullName: %s", sn.FullName)
	}
	if sn.Region != "cn" {
		t.Errorf("unexpected Region: %s", sn.Region)
	}
}

func TestEndpoint_JSON(t *testing.T) {
	ep := Endpoint{
		Addr: "10.0.0.1:8080",
		Port: 8080,
		Meta: map[string]string{"version": "v1"},
	}
	data, _ := json.Marshal(ep)
	var result map[string]any
	_ = json.Unmarshal(data, &result)

	if result["addr"] != "10.0.0.1:8080" {
		t.Errorf("unexpected addr: %v", result["addr"])
	}
	if result["port"].(float64) != 8080 {
		t.Errorf("unexpected port: %v", result["port"])
	}
}

func TestLbConstants(t *testing.T) {
	if LbRoundRobin != "roundrobin" {
		t.Errorf("unexpected LbRoundRobin: %s", LbRoundRobin)
	}
	if LbRandom != "random" {
		t.Errorf("unexpected LbRandom: %s", LbRandom)
	}
	if LbWeightedRandom != "weighted" {
		t.Errorf("unexpected LbWeightedRandom: %s", LbWeightedRandom)
	}
}
