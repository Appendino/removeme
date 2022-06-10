package main

import (
        "log"
        "net/http"
        "net/url"
        "fmt"
		"path"
        "time"
		"flag"
        "io"
		"strconv"
		"os"

        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promhttp"
)

func saveToFile(dirToSave string, localProm string){
	time.Sleep(10 * time.Second)
	u, _ := url.Parse("http://" + localProm + "/metrics")
	for{
		req := http.Request{
			Method: "GET",
			URL:    u,
		}
		resp, err := http.DefaultClient.Do(&req)
		if err != nil {
			fmt.Println("error to scrape", err)
		}else{
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			tempFilename := path.Join(dirToSave, "yarn-collector." + strconv.Itoa(os.Getpid()))
			finalFilename := path.Join(dirToSave, "yarn-collector.prom")
			f, err := os.Create(tempFilename)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
			defer f.Close()
			_, err = f.Write(b)
			if err != nil {
				log.Fatal("error to write metric files:", err)
				os.Exit(1)
			}
			err = os.Rename(tempFilename, finalFilename)
			if err != nil {
				log.Fatal("error to rename file:", err)
				os.Exit(1)
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func main() {
	var yarnUrl string
	var metricsEndpoint string
	var addr string
	var exportToNodeExporter bool
	var dirToSave string
	flag.StringVar(&yarnUrl, "yarn.address", "http://localhost:8088", "Url to collect metrics from Yarn")
	flag.StringVar(&metricsEndpoint, "metrics.endpoint", "ws/v1/cluster/metrics", "Endpoint to collect metrics from Yarn")
	flag.StringVar(&addr, "listen.address", "localhost:9113", "Address to listening")
	flag.BoolVar(&exportToNodeExporter, "node.exporter.textfile", false, "Save to file to node_exporter collect")
	flag.StringVar(&dirToSave, "textfile.directory", "/var/lib/node_exporter", "Save metrics to this directory")
	flag.Parse()

	fullMetricEndpoint := yarnUrl + "/" + metricsEndpoint
	endpoint, err := url.Parse(fullMetricEndpoint)	   
	if err != nil {
		log.Fatal("error to parse Yarn Url:", err)
	}

	if exportToNodeExporter {
        go saveToFile(dirToSave, addr)
	}

    c := newCollector(endpoint)
    err = prometheus.Register(c)
    if err != nil {
        log.Fatal(err)
    }

    http.Handle("/metrics", promhttp.Handler())
    log.Fatal(http.ListenAndServe(addr, nil))
}
