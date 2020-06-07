package main

import (
	"cbr-crawler/db"
	"cbr-crawler/models"
	"cbr-crawler/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	"io/ioutil"
	"net/http"
	url2 "net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
)

func main() {
	runtime.GOMAXPROCS(16)

	if err := utils.LoadConfig("config/config.yaml"); err != nil {
		fmt.Println(err)
	} else {
		db.Setup()
		if data, err := db.Database(); err != nil {
			fmt.Println(err)
		} else {
			db.ConfigConcurrency(data, 200)
			getAddress(data)
		}
	}
}

func getAddressFile() []models.HistoryAddresses {
	data, err := ioutil.ReadFile("address.csv")
	if err != nil {
		fmt.Println("File reading error", err)
		os.Exit(-1)
	}
	file := string(data)
	address := strings.Split(file, "\n")
	var modelsAddress = make([]models.HistoryAddresses, len(address))
	for i := range address {
		modelsAddress[i].RawAddress = address[i]
		addressTotal := strings.Split(modelsAddress[i].RawAddress, ",")
		var rawAddress string
		for j := 0; j < len(addressTotal); j++ {
			if j == (len(addressTotal) - 1) {
				modelsAddress[i].Key = addressTotal[j]
			} else {
				if j == 0 {
					rawAddress = addressTotal[j]
				} else {
					rawAddress = rawAddress + " " + addressTotal[j]
				}
			}
		}
		modelsAddress[i].RawAddress = rawAddress
	}

	return modelsAddress
}

func getAddress(data *gorm.DB) {
	var addresses []models.HistoryAddresses
	addresses = getAddressFile()
	//data.Select("DISTINCT(raw_address)").Find(&addresses)

	var ctx = make([]context.Context, 200)
	var client = make([]*http.Client, 200)
	for i := 0; i < 200; i++ {
		ctx[i] = context.Background()
		client[i] = new(http.Client)
		client[i].Timeout = 24 * time.Hour
	}

	var index int
	for index < len(addresses) {
		limit := 200
		if (len(addresses) - index) < 200 {
			limit = len(addresses) - index
		}
		var wg sync.WaitGroup
		wg.Add(limit)
		for i := 0; i < limit; i++ {
			go func(i, j int) {
				defer wg.Done()
				if addresses[i].RawAddress != "" && addresses[i].RawAddress != "-" &&
					addresses[i].RawAddress != "=" && addresses[i].RawAddress != "=-" {
					//if err := data.Where("raw_address = ?", addresses[i].RawAddress).Find(&addresses[i]).Error; err != nil {
					//	return
					//}
					//
					//var cbrAddress models.CbrAddresses
					//cbrAddress.Id = addresses[i].CbrAddressesId
					//if err := data.Where(cbrAddress).Find(&cbrAddress).Error; err != nil {
					//	return
					//}
					//
					//var digitalAddress models.DigitalesAddresses
					//digitalAddress.Id = cbrAddress.DigitalesAddressesId
					//if err := data.Where(digitalAddress).Find(&digitalAddress).Error; err != nil {
					//	return
					//}
					addressComplete := fmt.Sprintf("%s %s", addresses[i].RawAddress, addresses[i].Key)
					lat, long := getLatLng(client[j], ctx[j], addressComplete, addresses[i].Key)

					if strings.Compare(lat, "") == 0 && strings.Compare(long, "") == 0 {
						//fmt.Printf("%s\n", addressComplete)
					} else {
						fmt.Printf("%s,%s\n", lat, long)
					}
				}
			}(index, i)
			index++
		}
		wg.Wait()
	}
}

type OSM []struct {
	PlaceID     int      `json:"place_id"`
	Licence     string   `json:"licence"`
	OsmType     string   `json:"osm_type"`
	OsmID       int      `json:"osm_id"`
	Boundingbox []string `json:"boundingbox"`
	Lat         string   `json:"lat"`
	Lon         string   `json:"lon"`
	DisplayName string   `json:"display_name"`
	Class       string   `json:"class"`
	Type        string   `json:"type"`
	Importance  float64  `json:"importance"`
}

func getLatLng(client *http.Client, ctx context.Context, addressOrigin, commune string) (string, string) {
	address := tokenAddress(addressOrigin)
	var qAddress string
	if !strings.Contains(strings.ToLower(address), strings.ToLower(commune)) {
		qAddress = fmt.Sprintf("%s %s %s", address, commune, "Santiago Metropolitan Region")
	} else {
		qAddress = fmt.Sprintf("%s %s", address, "Santiago Metropolitan Region")
	}
	qAddress = strings.Replace(qAddress, ",", " ", -1)
	qAddress = strings.Replace(qAddress, " ", "+", -1)
	qAddress = strings.Replace(qAddress, "++", "+", -1)
	//fmt.Println(qAddress)
	var url = "http://127.0.0.1:7070/search?q=" + qAddress + "&format=json"
	lat, lng := searchLatLng(ctx, client, url, commune)
	if lat != "" && lng != "" {
		return lat, lng
	} else {
		var qAddress string
		address = removeAddress(address)
		if !strings.Contains(strings.ToLower(address), strings.ToLower(commune)) {
			qAddress = fmt.Sprintf("%s %s %s", address, commune, "Santiago Metropolitan Region")
		} else {
			qAddress = fmt.Sprintf("%s %s", address, "Santiago Metropolitan Region")
		}
		qAddress = strings.Replace(qAddress, ",", " ", -1)
		qAddress = strings.Replace(qAddress, " ", "+", -1)
		qAddress = strings.Replace(qAddress, "++", "+", -1)
		//fmt.Println(qAddress)
		var url = "http://127.0.0.1:7070/search?q=" + qAddress + "&format=json"
		return searchLatLng(ctx, client, url, commune)
	}
}

func searchLatLng(ctx context.Context, client *http.Client, url string, commune string) (string, string) {
	if urlParse, err := url2.Parse(url); err != nil {
		//fmt.Printf("parse url: %v", err.Error())
	} else {
		if reqGet, err := http.NewRequestWithContext(ctx, "GET", urlParse.String(), nil); err != nil {
			//fmt.Printf("new req get: %v", err.Error())
		} else {
			if respGet, err := client.Do(reqGet); err != nil {
				//fmt.Printf("client do get: %v", err.Error())
			} else {
				defer respGet.Body.Close()
				if respBody, err := ioutil.ReadAll(respGet.Body); err != nil {
					//fmt.Printf("ioutil: %v", err)
				} else {
					var resp OSM
					if err := json.Unmarshal(respBody, &resp); err != nil {
						//fmt.Printf("unmarshal %v", err)
					} else {
						if len(resp) > 0 {
							for i := 0; i < len(resp); i++ {
								//fmt.Println(resp[i].DisplayName)
								if strings.Contains(strings.ToLower(resp[0].DisplayName), strings.ToLower(commune)) {
									return resp[0].Lat, resp[0].Lon
								}
							}
						}
					}
				}
			}
		}
	}
	return "", ""
}

func tokenAddress(address string) string {
	var newAddress string
	address = strings.ToLower(address)
	address = strings.Replace(address, ",", " ", -1)
	address = strings.Replace(address, ".", " ", -1)
	splitAddress := strings.Split(address, " ")
	for i := 0; i < len(splitAddress); i++ {
		if i != 0 && i != 1 {
			if containNumber(splitAddress[i]) {
				newAddress = fmt.Sprintf("%s %s", newAddress, splitAddress[i])
				return newAddress
			} else {
				newAddress = fmt.Sprintf("%s %s", newAddress, splitAddress[i])
			}
		} else {
			if i == 0 {
				newAddress = splitAddress[i]
			} else {
				newAddress = fmt.Sprintf("%s %s", newAddress, splitAddress[i])
			}
		}
	}

	newAddress = dictionaryAddress(newAddress)

	return newAddress
}

func removeAddress(address string) string {
	address = strings.ToLower(address)
	address = strings.Replace(address, ".", " ", -1)
	address = strings.Replace(address, ",", " ", -1)
	//fmt.Println("remove: ", address)

	if strings.Contains(address, "pasaje") {
		address = strings.Replace(address, "pasaje", "", -1)
	} else if strings.Contains(address, "psaje") {
		address = strings.Replace(address, "psaje", "", -1)
	} else if strings.Contains(address, "psj") {
		address = strings.Replace(address, "psj", "", -1)
	} else if strings.Contains(address, "pje") {
		address = strings.Replace(address, "pje", "", -1)
	}

	if strings.Contains(address, "avenida") {
		address = strings.Replace(address, "avenida", "", -1)
	} else if strings.Contains(address, "avda") {
		address = strings.Replace(address, "avda", "", -1)
	} else if strings.Contains(address, "avd") {
		address = strings.Replace(address, "avd", "", -1)
	} else if strings.Contains(address, "av") {
		address = strings.Replace(address, "av", "", -1)
	}

	if strings.Contains(address, "calle") {
		address = strings.Replace(address, "calle", "", -1)
	}

	return address
}

func dictionaryAddress(address string) string {
	address = strings.Replace(address, ".", " ", -1)

	if strings.Contains(address, "psaje") {
		address = strings.Replace(address, "psaje", "pasaje", -1)
	} else if strings.Contains(address, "psj") {
		address = strings.Replace(address, "psj", "pasaje", -1)
	}

	if strings.Contains(address, "avd") {
		address = strings.Replace(address, "avd", "avenida", -1)
	} else if strings.Contains(address, "avda") {
		address = strings.Replace(address, "avda", "avenida", -1)
	} else if strings.Contains(address, "av") {
		address = strings.Replace(address, "av", "avenida", -1)
	}

	return address
}

func containNumber(s string) bool {
	for _, r := range s {
		if unicode.IsNumber(r) {
			return true
		}
	}
	return false
}
