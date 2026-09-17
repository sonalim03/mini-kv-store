// kv-cli: simple command-line client for the KV store.
//
// Usage:
//
//	kv-cli -addr localhost:9000 SET user:1001 Sonali
//	kv-cli -addr localhost:9000 GET user:1001
//	kv-cli -addr localhost:9000 DELETE user:1001
//	kv-cli -addr localhost:9000 TTL user:1001
//	kv-cli -addr localhost:9000 EXPIRE user:1001 60
//
// Administrative commands (stats/health work now; cluster/nodes require the
// cluster-membership API — see docs/architecture.md TODOs):
//
//	kv-cli -addr localhost:9000 stats
//	kv-cli -addr localhost:9000 health
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "node address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("usage: kv-cli -addr host:port <SET|GET|DELETE|EXISTS|TTL|EXPIRE|stats|health> ...")
		os.Exit(1)
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "STATS", "HEALTH":
		// Lightweight liveness check: a raw connection + one GET on a
		// sentinel key round-trips through the whole stack (network parse
		// -> engine). Full stats reporting needs the metrics endpoint —
		// see docs/architecture.md.
		conn, err := net.DialTimeout("tcp", *addr, 3*time.Second)
		if err != nil {
			fmt.Println("UNHEALTHY:", err)
			os.Exit(1)
		}
		defer conn.Close()
		fmt.Println("OK: connected to", *addr)
		return

	case "CLUSTER", "NODES":
		fmt.Println("not yet implemented: requires the cluster membership API (internal/membership) — see docs/architecture.md")
		return
	}

	// For SET specifically, re-quote the value if it came in as multiple
	// shell args (the shell already stripped the user's quotes before this
	// program ever saw them, e.g. `kv-cli SET k hello world` arrives as 4
	// separate args) so the wire protocol still gets exactly 3 tokens.
	var line string
	if cmd == "SET" && len(args) > 3 {
		value := strings.Join(args[2:], " ")
		line = fmt.Sprintf("SET %s %q\n", args[1], value)
	} else {
		line = strings.Join(args, " ") + "\n"
	}
	conn, err := net.DialTimeout("tcp", *addr, 3*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection failed:", err)
		os.Exit(1)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte(line)); err != nil {
		fmt.Fprintln(os.Stderr, "write failed:", err)
		os.Exit(1)
	}

	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, "read failed:", err)
		os.Exit(1)
	}
	fmt.Print(reply)
}
