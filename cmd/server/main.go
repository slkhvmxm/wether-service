package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5"
	"github.com/slkhvmxm/wether-service/internal/client/http/geocoding"
	"github.com/slkhvmxm/wether-service/internal/client/http/open_meteo"
)

const (
	city     = "moscow"
	httpPort = ":3000"
)

type Reading struct {
	Name        string    `db:"name"`
	Timestamp   time.Time `db:"timestamp"`
	Temperature float64   `db:"temperature"`
}

func CreateTables(conn *pgx.Conn) error {
	query := `
	CREATE TABLE IF NOT EXISTS reading (
		name text not null,
		timestamp timestamp not null,
		temperature float8 not null
	);
	`
	_, err := conn.Exec(context.Background(), query)
	return err
}
func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgresql://maximka:parol@localhost:54321/weather")
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	err = CreateTables(conn)
	if err != nil {
		log.Fatal("Failed to create tables:", err)
	}

	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		cityName := chi.URLParam(r, "city")

		fmt.Printf("requested city: %s\n", cityName)

		var reading Reading
		err = conn.QueryRow(
			ctx,
			"select name, timestamp, temperature from reading where name = $1 order by timestamp desc limit 1", city,
		).Scan(&reading.Name, &reading.Timestamp, &reading.Temperature)
		if err != nil {
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
			}
		}

		var raw []byte
		raw, err := json.Marshal(reading)
		if err != nil {
			log.Println(err)
		}

		_, err = w.Write([]byte(raw))
		if err != nil {
			log.Println(err)
		}

	})

	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}
	jobs, err := initJobs(ctx, s, conn)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		fmt.Println("starting server on port " + httpPort)
		if err = http.ListenAndServe(httpPort, r); err != nil {
			panic(err)
		}

	}()

	go func() {
		defer wg.Done()
		fmt.Printf("starting job: %v", jobs[0].ID())
		s.Start()
	}()

	wg.Wait()
}

func initJobs(ctx context.Context, scheduler gocron.Scheduler, conn *pgx.Conn) ([]gocron.Job, error) {
	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	openMeteoClient := open_meteo.NewClient(httpClient)

	j, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				geoRes, err := geocodingClient.GetCoords(city)
				if err != nil {
					log.Panicln(err)
					return
				}

				openMetRes, err := openMeteoClient.GetTemperature(geoRes.Latitude, geoRes.Longitude)
				if err != nil {
					log.Println(err)
					return
				}

				timestamp, err := time.ParseInLocation("2006-01-02T15:04", openMetRes.Current.Time, time.Local)
				if err != nil {
					log.Println(err)
					return
				}
				timestamp = timestamp.Add(3 * time.Hour)
				_, err = conn.Exec(
					ctx,
					"insert into reading (name, temperature, timestamp) values ($1, $2, $3)",
					city, openMetRes.Current.Temperature2m, timestamp,
				)
				if err != nil {
					log.Println(err)
					return
				}
				fmt.Printf("%v updated data for city: %s\n", time.Now(), city)
			},
		),
	)
	if err != nil {
		return nil, err
	}
	return []gocron.Job{j}, nil
}
