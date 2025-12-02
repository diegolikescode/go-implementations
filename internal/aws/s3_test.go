package aws

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/diegolikescode/go-data-streaming/internal/metrics"
)

/*
   00 id
   01 country
   02 description
   03 designation
   04 points
   05 price
   06 province
   07 region_1
   08 region_2
   09 taster_name
   10 taster_twitter_handle
   11 title
   12 variety
   13 winery
*/

func TestStreamLocalFileToBucket(t *testing.T) {
	return
	mem := &metrics.MemObserver{}
	mem.PrintMemoryUsage("Start")

	filename := "./testdata/wine-dataset.csv"
	newFilename := "result/wine-dataset-final.csv"

	s3Manager := NewS3Manager("")

	callback := func(r io.ReadCloser, w io.Writer) error {
		defer r.Close()
		mem.PrintMemoryUsage("inside transformer callback")
		csvReader := csv.NewReader(r)

		lineCounter := 1
		l, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Log("the file is empty")
				return nil
			}
			t.Logf("error while reading file (line %d): %v", lineCounter, err)
			return err
		}

		l[0] = "id"

		mem.PrintMemoryUsage("starts transformation loop")
		for {
			l, err = csvReader.Read()
			lineCounter++
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Logf("error while reading file (line %d): %v", lineCounter, err)
				return err
			}
			l[1] = "BRASIL_BABY, " + l[1]

			w.Write([]byte(strings.Join(l, ",")))
			if lineCounter%10000 == 0 {
				mem.PrintMemoryUsage("inside transform loop")
			}
		}

		mem.PrintMemoryUsage("transformation completed")
		return nil
	}

	s3Manager.StreamLocalFileToBucket(context.Background(), filename, newFilename, callback)
}

func TestStreamContent(t *testing.T) {
	mem := &metrics.MemObserver{}
	mem.PrintMemoryUsage("Start")

	content := "Nunca é demais lembrar o peso e o significado destes problemas, uma vez que a consolidação das estruturas assume importantes posições no estabelecimento das diversas correntes de pensamento."
	reader := strings.NewReader(content)

	newFilename := "result/inmemory-content.txt"

	s3Manager := NewS3Manager("")

	callback := func(r io.Reader, w io.WriteCloser) error {
		mem.PrintMemoryUsage("inside transformer callback")

		b := make([]byte, 8)
		n := 1
		mem.PrintMemoryUsage("starts transformation loop")
		for n > 0 {
			n, err := r.Read(b)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Logf("error while reading file: %v", err)
				return err
			}

			w.Write(b[:n])
		}

		mem.PrintMemoryUsage("transformation completed")
		return nil
	}

	s3Manager.StreamWithTransform(context.Background(), reader, newFilename, callback)
}
