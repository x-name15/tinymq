package handle

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

func HandleCluster(baseURL string, args []string) {
	if len(args) == 0 {
		fmt.Println("Use: tmq cluster status\n    tmq cluster peers [--watch]\n    tmq cluster drain <node-url>")
		return
	}
	switch args[0] {
	case "status":
		printClusterStatus(baseURL)
	case "peers":
		peersCmd := flag.NewFlagSet("peers", flag.ExitOnError)
		watch := peersCmd.Bool("watch", false, "Continuously refresh the peer table every 2s")
		peersCmd.Parse(args[1:])
		if *watch {
			for {
				fmt.Print("\033[H\033[2J")
				printClusterStatus(baseURL)
				fmt.Printf("\n%s(Refreshing every 2s. Press Ctrl+C to exit)%s\n", shared.ColorYellow, shared.ColorReset)
				time.Sleep(2 * time.Second)
			}
		}
		printClusterStatus(baseURL)
	case "drain":
		if len(args) < 2 {
			fmt.Println("Use: tmq cluster drain <node-url>")
			return
		}
		target := args[1]
		resp, err := shared.DoAuthRequest(http.MethodPost, target+"/api/drain", nil)
		if err != nil {
			fmt.Printf("%s[Error] %v%s\n", shared.ColorRed, err, shared.ColorReset)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s✔ Drain requested on %s%s\n%s\n", shared.ColorGreen, target, shared.ColorReset, string(body))
	default:
		fmt.Println("Use: tmq cluster status\n    tmq cluster peers [--watch]\n    tmq cluster drain <node-url>")
	}
}

func printClusterStatus(baseURL string) {
	resp, err := shared.DoAuthRequest(http.MethodGet, baseURL+"/api/cluster/status", nil)
	if err != nil {
		fmt.Printf("%s[Error] Error connecting to the broker at %s: %v%s\n", shared.ColorRed, baseURL, err, shared.ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", shared.ColorRed, shared.ColorReset)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	var status shared.ClusterStatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Printf("%s[Error] Could not parse cluster status response: %v%s\n", shared.ColorRed, err, shared.ColorReset)
		return
	}
	if !status.ClusteringEnabled {
		fmt.Println("Clustering is not enabled on this node (standalone mode).")
		return
	}
	roleColor := shared.ColorGreen
	switch status.Role {
	case "candidate":
		roleColor = shared.ColorYellow
	case "follower":
		roleColor = shared.ColorBlue
	}
	fmt.Printf("\n%sTINYMQ CLUSTER STATUS (%s)%s\n\n", shared.ColorBold+shared.ColorCyan, baseURL, shared.ColorReset)
	fmt.Printf("Role:  %s%s%s\n", roleColor, status.Role, shared.ColorReset)
	fmt.Printf("Term:  %d\n", status.Term)
	if status.Role != "leader" {
		leaderStr := status.LeaderHTTP
		if leaderStr == "" {
			leaderStr = shared.ColorYellow + "unknown (electing)" + shared.ColorReset
		}
		fmt.Printf("Leader: %s\n", leaderStr)
	}
	if len(status.Peers) == 0 {
		fmt.Println("\nNo peers configured.")
		return
	}
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintf(w, "%sADDRESS\tSTATUS\tLAST SEEN%s\n", shared.ColorBold, shared.ColorReset)
	for _, p := range status.Peers {
		statusStr := shared.ColorRed + "dead" + shared.ColorReset
		if p.Alive {
			statusStr = shared.ColorGreen + "alive" + shared.ColorReset
		}
		lastSeen := "never"
		if !p.LastSeen.IsZero() {
			lastSeen = time.Since(p.LastSeen).Round(time.Second).String() + " ago"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", p.Address, statusStr, lastSeen)
	}
	w.Flush()
	fmt.Println()
}
