package experimental 

import (
	"io"
	"sync"
	"testing"
	"time"
	"net/http"

	"github.com/snowplow/snowbridge/cmd"
	"github.com/snowplow/snowbridge/cmd/cli"
	"github.com/snowplow/snowbridge/config"
	inmemorysource "github.com/snowplow/snowbridge/pkg/source/inmemory"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

const url = "localhost:10000"

func TestApp1(t *testing.T) {
	 assert := assert.New(t)
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", "./test_config.hcl")
	inputMessages := make(chan []string)
	outputMessages := []string{}
	wg := sync.WaitGroup{}

  run(inputMessages, &outputMessages, &wg)

	wg.Add(5)
	inputMessages <- []string{"mes1", "mes2"}
  time.Sleep(2 * time.Second)
	inputMessages <- []string{"mes3"}
  time.Sleep(2 * time.Second)
	inputMessages <- []string{"mes4", "mes5"}
	wg.Wait()

  assert.Equal([]string{"mes1", "mes2", "mes3", "mes4", "mes5"}, outputMessages)
}

func run(input chan []string, output *[]string, wg *sync.WaitGroup) {
	runHTTPServer(output, wg)

	sourceConfigPairs := []config.ConfigurationPair{inmemorysource.ConfigPair(input)}

	config, _, _ := cmd.Init()
  //TODO handle cancellation/stopping better
	go func() {
    err := cli.RunApp(config, sourceConfigPairs, transformconfig.SupportedTransformations)
    panic(err)
  }()
}

//Use httptest
func runHTTPServer(output *[]string, wg *sync.WaitGroup) {
	mutex := &sync.Mutex{}
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		data, err := io.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
    mutex.Lock()
		*output = append(*output, string(data))
    mutex.Unlock()
		defer wg.Done()
	})
  go http.ListenAndServe(url, nil)
}


