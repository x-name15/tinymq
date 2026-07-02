package handle

import (
	"fmt"
	"github.com/x-name15/tinymq/cmd/tmq/shared"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func HandleDoctor(baseURL string, args []string) {
	fmt.Printf("\n%s🩺 TinyMQ Doctor%s\n\n", shared.ColorBold+shared.ColorCyan, shared.ColorReset)

	checks := 0
	warnings := 0
	failures := 0

	report := func(ok bool, warn bool, label, detail string) {
		checks++
		switch {
		case ok:
			fmt.Printf("  %s✔%s %s\n", shared.ColorGreen, shared.ColorReset, label)
		case warn:
			warnings++
			fmt.Printf("  %s⚠%s %s%s\n", shared.ColorYellow, shared.ColorReset, label, suffixDetail(detail))
		default:
			failures++
			fmt.Printf("  %s✘%s %s%s\n", shared.ColorRed, shared.ColorReset, label, suffixDetail(detail))
		}
	}

	report(true, false, fmt.Sprintf("Go runtime: %s", runtime.Version()), "")

	dataDir := "./data"
	checkDataDir(dataDir, report)

	if _, err := os.Stat(".env"); err == nil {
		report(true, false, ".env file found in current directory", "")
	} else {
		report(true, true, "No .env file found in current directory", "using shell-exported environment variables, or defaults")
	}

	if os.Getenv("TINYMQ_API_KEY") == "" {
		report(true, true, "TINYMQ_API_KEY is not set", "the broker and dashboard will be open to anyone who can reach the port")
	} else {
		report(true, false, "TINYMQ_API_KEY is set", "")
	}

	certFile := os.Getenv("TINYMQ_TLS_CERT")
	keyFile := os.Getenv("TINYMQ_TLS_KEY")
	switch {
	case certFile == "" && keyFile == "":
		report(true, false, "TLS disabled (TINYMQ_TLS_CERT/TINYMQ_TLS_KEY not set)", "")
	case certFile != "" && keyFile != "":
		report(true, false, "TLS configured", "")
	default:
		report(false, false, "TLS misconfigured", "TINYMQ_TLS_CERT and TINYMQ_TLS_KEY must both be set, or both left empty")
	}

	clusterAddr := os.Getenv("TINYMQ_CLUSTER_ADDR")
	if clusterAddr != "" {
		if os.Getenv("TINYMQ_CLUSTER_SECRET") == "" && os.Getenv("TINYMQ_CLUSTER_ALLOW_INSECURE") != "true" {
			report(false, false, "Clustering enabled without TINYMQ_CLUSTER_SECRET", "set TINYMQ_CLUSTER_SECRET or explicitly set TINYMQ_CLUSTER_ALLOW_INSECURE=true")
		} else {
			report(true, false, "Cluster configuration looks consistent", "")
		}
	}

	httpPort := os.Getenv("PORT")
	if httpPort == "" {
		httpPort = "7800"
	}
	checkPort(httpPort, "HTTP/WS", report)

	if os.Getenv("TINYMQ_MQTT_DISABLE") != "true" {
		mqttPort := os.Getenv("TINYMQ_MQTT_PORT")
		if mqttPort == "" {
			mqttPort = "1883"
		}
		checkPort(mqttPort, "MQTT", report)
	}

	if natsPort := os.Getenv("TINYMQ_NATS_PORT"); natsPort != "" {
		checkPort(natsPort, "NATS", report)
	}

	checkBrokerReachable(baseURL, report)

	fmt.Println()
	switch {
	case failures > 0:
		fmt.Printf("%s%d check(s) failed, %d warning(s). Fix the ✘ items before starting the broker.%s\n\n", shared.ColorRed, failures, warnings, shared.ColorReset)
	case warnings > 0:
		fmt.Printf("%sAll critical checks passed, %d warning(s) to review.%s\n\n", shared.ColorYellow, warnings, shared.ColorReset)
	default:
		fmt.Printf("%sAll %d checks passed. TinyMQ should start cleanly.%s\n\n", shared.ColorGreen, checks, shared.ColorReset)
	}
}

func suffixDetail(detail string) string {
	if detail == "" {
		return ""
	}
	return fmt.Sprintf(" %s(%s)%s", shared.ColorReset, detail, shared.ColorReset)
}

func checkDataDir(dataDir string, report func(ok, warn bool, label, detail string)) {
	info, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		report(true, true, fmt.Sprintf("Data directory '%s' does not exist yet", dataDir), "it will be created automatically on first run")
		return
	}
	if err != nil {
		report(false, false, fmt.Sprintf("Cannot stat data directory '%s'", dataDir), err.Error())
		return
	}
	if !info.IsDir() {
		report(false, false, fmt.Sprintf("'%s' exists but is not a directory", dataDir), "")
		return
	}
	probe := filepath.Join(dataDir, ".doctor-write-test")
	f, err := os.Create(probe)
	if err != nil {
		report(false, false, fmt.Sprintf("Data directory '%s' is not writable", dataDir), err.Error())
		return
	}
	f.Close()
	os.Remove(probe)
	report(true, false, fmt.Sprintf("Data directory '%s' exists and is writable", dataDir), "")
}

func checkPort(port, label string, report func(ok, warn bool, label, detail string)) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		report(false, false, fmt.Sprintf("%s port %s is already in use", label, port), "stop whatever is bound to it, or reconfigure the port via env vars")
		return
	}
	ln.Close()
	report(true, false, fmt.Sprintf("%s port %s is free", label, port), "")
}

func checkBrokerReachable(baseURL string, report func(ok, warn bool, label, detail string)) {
	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Get(baseURL + "/healthz")
	if err != nil {
		report(true, true, fmt.Sprintf("No broker currently reachable at %s", baseURL), "expected if you haven't started it yet")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		report(true, false, fmt.Sprintf("Broker already running and healthy at %s", baseURL), "")
	} else {
		report(true, true, fmt.Sprintf("Broker reachable at %s but not healthy yet", baseURL), fmt.Sprintf("status %d, likely mid-election", resp.StatusCode))
	}
}
