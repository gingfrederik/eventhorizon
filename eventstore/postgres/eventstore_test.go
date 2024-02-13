package postgres

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/eventstore/postgres/sqlc/db"
	"github.com/gingfrederik/eventhorizon/mocks"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

var connPool *pgxpool.Pool

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	randomPort, err := GetFreePort()
	if err != nil {
		log.Fatalf("Could not get free port: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=abcd1234",
			"POSTGRES_USER=admin",
			"POSTGRES_DB=eventhorizon",
			"listen_addresses = '*'",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d/tcp", randomPort)}},
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://admin:abcd1234@%s/eventhorizon?sslmode=disable", hostAndPort)

	log.Println("Connecting to database on url: ", databaseUrl)

	resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		connPool, err = pgxpool.New(context.Background(), databaseUrl)
		if err != nil {
			return err
		}

		return connPool.Ping(context.Background())
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	//Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func GetFreePort() (port int, err error) {
	var a *net.TCPAddr

	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()

			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}

	return
}

func TestEventStore_Save(t *testing.T) {
	eventStore, err := NewEventStoreWithConnPool(context.Background(), connPool)
	if err != nil {
		t.Fatal("NewEventStoreWithConn error:", err)
	}

	type args struct {
		events          []eh.Event
		stream          db.Stream
		originalVersion int
	}

	id := uuid.New()
	event1 := eh.NewEventForAggregate(
		mocks.EventType,
		&mocks.EventData{Content: "event1"},
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		mocks.AggregateType,
		id,
		1)

	event2 := eh.NewEventForAggregate(
		mocks.EventType,
		&mocks.EventData{Content: "event2"},
		time.Date(2009, time.November, 10, 23, 10, 0, 0, time.UTC),
		mocks.AggregateType,
		id,
		2)

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				events: []eh.Event{event1, event2},
				stream: db.Stream{
					ID:            id,
					AggregateType: mocks.AggregateType.String(),
					Position:      2,
					Version:       event2.Version(),
					UpdatedAt:     event2.Timestamp(),
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := eventStore.Save(context.Background(), tt.args.events, tt.args.originalVersion); (err != nil) != tt.wantErr {
				t.Errorf("EventStore.Save() error = %v, wantErr %v", err, tt.wantErr)
			}

			events, err := eventStore.Load(context.Background(), id)
			if err != nil {
				t.Fatal("EventStore.Load error:", err)
			}

			if !cmp.Equal(tt.args.events, events, cmp.Comparer(compareEvent)) {
				t.Error("events are not equal:", cmp.Diff(tt.args.events, events))
			}

			dbq := db.New(connPool)

			stream, err := dbq.GetStream(context.Background(), id)
			if err != nil {
				t.Error("GetStream error:", err)
			}

			if !cmp.Equal(tt.args.stream, stream) {
				t.Error("streams are not equal:", cmp.Diff(tt.args.stream, stream))
			}
		})
	}
}

func compareEvent(a, b eh.Event) bool {
	if a.EventType() != b.EventType() {
		return false
	}

	if a.Timestamp() != b.Timestamp() {
		return false
	}

	if a.AggregateType() != b.AggregateType() {
		return false
	}

	if a.AggregateID() != b.AggregateID() {
		return false
	}

	if a.Version() != b.Version() {
		return false
	}

	if !cmp.Equal(a.Data(), b.Data()) {
		return false
	}

	if !cmp.Equal(a.Metadata(), b.Metadata()) {
		return false
	}

	return true
}
