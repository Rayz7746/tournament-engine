package checkin

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"tournament-engine/pkg/testutil"
)

var (
	testRedisAddress string
	testPostgresDSN  string
)

func TestMain(m *testing.M) {
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 2*time.Minute)
	environment, err := testutil.SetupTestContainers(setupCtx)
	cancelSetup()
	if err != nil {
		log.Printf("set up check-in test containers: %v", err)
		os.Exit(1)
	}

	testRedisAddress = environment.RedisAddr
	testPostgresDSN = environment.PostgresDSN
	exitCode := m.Run()

	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
	if err := environment.Terminate(cleanupCtx); err != nil {
		log.Printf("terminate check-in test containers: %v", err)
		exitCode = 1
	}
	cancelCleanup()
	os.Exit(exitCode)
}
