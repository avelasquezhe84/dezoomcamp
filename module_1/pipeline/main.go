package main

import (
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DateTime struct {
	time.Time
}

type Flag string

const (
	Y Flag = "Y"
	N Flag = "N"
)

type Entry struct {
	gorm.Model

	VendorID             uint     `csv:"VendorID"`
	TpepPickup           DateTime `csv:"tpep_pickup_datetime"`
	TpepDropoff          DateTime `csv:"tpep_dropoff_datetime"`
	PassengerCount       uint     `csv:"passenger_count"`
	TripDistance         float32  `csv:"trip_distance"`
	RatecodeID           uint     `csv:"RatecodeID"`
	StoreAndFwdFlag      Flag     `csv:"store_and_fwd_flag"`
	PULocationID         uint     `csv:"PULocationID"`
	DOLocationID         uint     `csv:"DOLocationID"`
	PaymentType          uint     `csv:"payment_type"`
	FareAmount           uint     `csv:"fare_amount"`
	Extra                uint     `csv:"extra"`
	MtaTax               float32  `csv:"mta_tax"`
	TipAmount            uint     `csv:"tip_amount"`
	TollsAmount          uint     `csv:"tolls_amount"`
	ImprovementSurcharge float32  `csv:"improvement_surcharge"`
	TotalAmount          float32  `csv:"total_amount"`
	CongestionSurcharge  float32  `csv:"congestion_surcharge"`
}

func main() {
	start := time.Now()
	log.Println("Initializing ingestion...")
	log.Println("Opening CSV file...")
	file, err := os.Open("/home/ubuntu/Projects/DataTalks/DEZoomcamp/module_1/ny_taxi_csv_data/yellow_tripdata_2021-01_100.csv")
	if err != nil {
		log.Fatal("Error loading file...", err)
	}

	defer file.Close()

	log.Println("Loading CSV data...")
	var entries []Entry
	err = gocsv.Unmarshal(file, &entries)
	if err != nil {
		log.Fatal("Error loading file...", err)
	}

	log.Println("Connecting to database...")
	db, err := gorm.Open(postgres.Open("host=localhost user=root password=root dbname=ny_taxi port=5432 sslmode=disable"))
	if err != nil {
		log.Fatal("Error connecting to db...", err)
	}

	log.Println("Creating table if not exists...")
	err = db.AutoMigrate(&Entry{})
	if err != nil {
		log.Fatal("Error creating table...", err)
	}

	log.Println("Loading data to table...")
	result := db.Create(entries)
	if result.Error != nil {
		log.Fatal("Error inserting data...", err)
	}

	elapsed := time.Since(start)

	log.Println("Finished...")
	log.Printf("Process took %s", elapsed)
}
