package sparkicenessie

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestEndToEndTableFlow(t *testing.T) {
	ctx := context.Background()

	// 1. Create a Docker Network with a unique name
	netName := fmt.Sprintf("test-network-%d", time.Now().UnixNano())
	netReq := testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name: netName,
		},
	}
	net, err := testcontainers.GenericNetwork(ctx, netReq)
	if err != nil {
		t.Fatalf("failed to create network: %s", err)
	}
	defer net.Remove(ctx)

	// 2. Start MinIO Container
	minioReq := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		Cmd:          []string{"server", "/data"},
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "admin",
			"MINIO_ROOT_PASSWORD": "password",
		},
		Networks: []string{netName},
		NetworkAliases: map[string][]string{
			netName: {"minio"},
		},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
	}
	minioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: minioReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start minio: %s", err)
	}
	defer minioC.Terminate(ctx)

	// 3. Start Nessie Container
	nessieReq := testcontainers.ContainerRequest{
		Image:        "projectnessie/nessie:latest",
		ExposedPorts: []string{"19120/tcp"},
		Networks:     []string{netName},
		NetworkAliases: map[string][]string{
			netName: {"nessie"},
		},
		WaitingFor: wait.ForHTTP("/api/v1/config").WithPort("19120/tcp"),
	}
	nessieC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: nessieReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start nessie: %s", err)
	}
	defer nessieC.Terminate(ctx)

	// 3.5 Create Warehouse Bucket in MinIO
	_, _, err = minioC.Exec(ctx, []string{"/bin/sh", "-c", "mc alias set myminio http://localhost:9000 admin password && mc mb myminio/warehouse"})
	if err != nil {
		t.Fatalf("failed to create warehouse bucket: %s", err)
	}

	// 4. Start Spark Container
	imageName := getTestImage()
	sparkReq := testcontainers.ContainerRequest{
		Image: imageName,
		Env: map[string]string{
			"NESSIE_ENDPOINT":       "http://nessie:19120/api/v1",
			"MINIO_ENDPOINT":        "http://minio:9000",
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "admin",
			"AWS_SECRET_ACCESS_KEY": "password",
		},
		Networks: []string{netName},
		Cmd:      []string{"tail", "-f", "/dev/null"},
	}

	sparkC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: sparkReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start spark: %s", err)
	}
	defer sparkC.Terminate(ctx)

	// 5. Execute E2E Spark SQL Commands (Create, Insert, Update, Select)
	pythonScript := `
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql('CREATE NAMESPACE IF NOT EXISTS nessie.db')
spark.sql('CREATE TABLE nessie.db.test (id int, data string) USING iceberg')
spark.sql("INSERT INTO nessie.db.test VALUES (1, 'initial-data')")
spark.sql("UPDATE nessie.db.test SET data = 'updated-nessie-data' WHERE id = 1")
spark.table('nessie.db.test').show()
`
	output := runPython(t, ctx, sparkC, pythonScript)
	t.Log(output)
	assert.Contains(t, output, "updated-nessie-data", "Query output should contain updated data")
	assert.NotContains(t, output, "initial-data", "Query output should NOT contain stale data after update")
}

func runPython(t *testing.T, ctx context.Context, c testcontainers.Container, pythonCode string) string {
	fullScript := fmt.Sprintf(`from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
%s
`, pythonCode)

	scriptPath := "/tmp/script.py"
	escapedCode := strings.ReplaceAll(fullScript, "'", "'\\''")
	cmd := []string{"/bin/bash", "-c", fmt.Sprintf("printf '%%s' '%s' > %s", escapedCode, scriptPath)}
	exitCode, _, err := c.Exec(ctx, cmd)
	if err != nil || exitCode != 0 {
		t.Fatalf("failed to write python script to container: %s (exit %d)", err, exitCode)
	}

	cmd = []string{"/bin/bash", "-c", fmt.Sprintf("timeout 300 /usr/local/bin/pyspark-iceberg-nessie-shell --verbose %s > /tmp/query_out 2>&1", scriptPath)}
	exitCode, _, err = c.Exec(ctx, cmd)
	if err != nil {
		t.Fatalf("failed to initiate python script: %s", err)
	}

	_, reader, err := c.Exec(ctx, []string{"cat", "/tmp/query_out"})
	if err != nil {
		t.Fatalf("failed to read query output file: %s", err)
	}
	b, _ := io.ReadAll(reader)
	output := string(b)

	if exitCode != 0 {
		t.Fatalf("query [%s] failed with output:\n%s", pythonCode, output)
	}

	return output
}

func getTestImage() string {
	name := os.Getenv("IMAGE_NAME")
	if name == "" {
		name = "rnalex/spark-iceberg-nessie"
	}
	tag := os.Getenv("TAG")
	if tag == "" {
		tag = "latest"
	}
	return fmt.Sprintf("%s:%s", name, tag)
}
