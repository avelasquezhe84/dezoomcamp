package main

import (
	"database/sql/driver"
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

func (date *DateTime) MarshalCSV() (string, error) {
	return date.Time.Format("2006-01-02 15:04:05"), nil
}

func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	date.Time, err = time.Parse("2006-01-02 15:04:05", csv)
	return err
}

func (date *DateTime) Scan(value string) error {
	return date.UnmarshalCSV(value)
}

func (date *DateTime) Value() (driver.Value, error) {
	return date.MarshalCSV()
}

type Flag string

const (
	Y Flag = "Y"
	N Flag = "N"
)

type TaxiTrip struct {
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

	file, err := os.OpenFile("/home/ubuntu/Projects/DataTalks/DEZoomcamp/module_1/ny_taxi_csv_data/yellow_tripdata_2021-01_100.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Fatal("ERROR loading file: ", err)
	}
	defer file.Close()

	log.Println("Loading CSV data...")

	taxiTrips := []*TaxiTrip{}
	if err := gocsv.UnmarshalFile(file, &taxiTrips); err != nil {
		log.Fatal("ERROR reading file: ", err)
	}

	log.Println("Connecting to database...")

	db, err := gorm.Open(postgres.Open("host=localhost user=root password=root dbname=ny_taxi port=5432 sslmode=disable"))
	if err != nil {
		log.Fatal("ERROR connecting to db: ", err)
	}

	log.Println("Creating table if not exists...")

	if err := db.AutoMigrate(&TaxiTrip{}); err != nil {
		log.Fatal("ERROR creating table: ", err)
	}

	log.Println("Loading data to table...")

	result := db.Create(taxiTrips)
	if result.Error != nil {
		log.Fatal("ERROR inserting data: ", err)
	}

	elapsed := time.Since(start)

	log.Println("Finished...")
	log.Printf("Process took %s", elapsed)
}
