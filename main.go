package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/fatih/structs"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {

	argumentos := os.Args
	// log.Println(argumentos)
	opcion := argumentos[1]
	switch opcion {
	case "1":
		s3WriteExample()
	case "2":
		s3ReadExample()
	case "3":
		s3WriteCSVFromParquetExample()
	default:
		log.Println("Error en los parametros de entrada")
	}
}

// s3Example provides a sample write and read using the S3 Parquet File
func s3WriteExample() {
	ctx := context.Background()
	bucket := "guild-go"
	key := "godxvincent/foobar.parquet"
	num := 100
	// cfgs := make(&aws.Config, 2)
	cfgs := aws.NewConfig().WithRegion("us-east-2")

	// create new S3 file writer
	fw, err := s3.NewS3FileWriter(ctx, bucket, key, nil, cfgs)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	// create new parquet file writer
	pw, err := writer.NewParquetWriter(fw, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	// write 100 student records to the parquet file
	for i := 0; i < num; i++ {
		stu := student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	// write parquet file footer
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop err", err)
	}

	err = fw.Close()
	if err != nil {
		log.Println("Error closing S3 file writer")
		log.Println(err)
	}
	log.Println("Write Finished")
}

func s3ReadExample() {

	ctx := context.Background()
	bucket := "guild-go"
	key := "godxvincent/foobar.parquet"
	num := 100
	cfgs := aws.NewConfig().WithRegion("us-east-2")

	// read the written parquet file
	// create new S3 file reader
	fr, err := s3.NewS3FileReader(ctx, bucket, key, cfgs)
	if err != nil {
		log.Println("Can't open file")
		return
	}

	// create new parquet file reader
	pr, err := reader.NewParquetReader(fr, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	// read the student rows and print
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	// close the parquet file
	pr.ReadStop()
	err = fr.Close()
	if err != nil {
		log.Println("Error closing S3 file reader")
	}
	log.Println("Read Finished")
}

func s3WriteCSVFromParquetExample() {
	ctx := context.Background()
	bucket := "guild-go"
	key := "godxvincent/foobar.parquet"
	num := 100
	cfgs := aws.NewConfig().WithRegion("us-east-2")

	// read the written parquet file
	// create new S3 file reader
	fr, err := s3.NewS3FileReader(ctx, bucket, key, cfgs)
	if err != nil {
		log.Println("Can't open file")
		return
	}

	// create new parquet file reader
	pr, err := reader.NewParquetReader(fr, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	// read the student rows and print
	f, e := os.Create("estudiantes.csv")
	if e != nil {
		log.Println(e)
	}
	writer := csv.NewWriter(f)
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]student, 10) //read 10 rows
		// log.Println("Len:", len(stus), "cap: ", cap(stus), stus)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		escribirCSV(stus, writer)
		// log.Println(stus)

	}

}

func escribirCSV(estudiantes []student, writer *csv.Writer) {

	log.Println("Va iniciar a grabar")

	numeroCampos := len(structs.Values(estudiantes[0]))
	log.Println(numeroCampos)
	// matriz := make([][]string, len(estudiantes))

	for _, estudiante := range estudiantes {
		log.Println(estudiante)
		arregloDatos := structs.Values(estudiante)
		log.Println("imprimiendo el contenido de arregloDatos", arregloDatos)
		stringStudiante := make([]string, 0)
		for _, valor := range arregloDatos {
			stringStudiante = append(stringStudiante, fmt.Sprintf("%v", valor))
		}
		log.Println(stringStudiante)
		e := writer.Write(stringStudiante)
		if e != nil {
			fmt.Println(e)
		}
		// matriz = append(matriz, stringStudiante)
	}
	// e := writer.WriteAll(matriz)
	writer.Flush()

	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}

}
