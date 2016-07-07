package openehr

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"errors"

	"bytes"

	"github.com/FiviumAustralia/RNSH-Pilot-Server-Go/models"
)

type OpenEHRManager struct {
	BaseUrl          string
	SubjectNamespace string
	Username         string
	Password         string
}

type partyInfoType struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type AddressType struct {
	Id      string `json:"id,omitempty"`
	Address string `json:"address"`
}

type Party struct {
	Id                  string          `json:"id,omitempty"`
	FirstNames          string          `json:"firstNames"`
	LastNames           string          `json:"lastNames"`
	Gender              string          `json:"gender"`
	DateOfBirth         string          `json:"dateOfBirth"`
	Address             AddressType     `json:"address"`
	PartyAdditionalInfo []partyInfoType `json:"partyAdditionalInfo"`
}

type PartiesType struct {
	Parties []Party `json:"parties"`
}

type PartyType struct {
	Party Party `json:"party"`
}

type ehrType struct {
	EhrId string `json:"ehrId"`
}

type hrefType struct {
	Href string `json:"href"`
}

type partyCreateReturn struct {
	Meta hrefType `json:"meta"`
}

func (oeps OpenEHRManager) getAuthorizationHeader() string {
	s := fmt.Sprintf("%s:%s", oeps.Username, oeps.Password)
	encoded := base64.StdEncoding.EncodeToString([]byte(s))
	return fmt.Sprintf("Basic %s", encoded)
}

func (oeps OpenEHRManager) GetOpenEhr(url string) []byte {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", oeps.getAuthorizationHeader())
	req.Header.Add("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	return body
}

func (oeps OpenEHRManager) PostOpenEhr(url string, body []byte) ([]byte, string, int) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Add("Authorization", oeps.getAuthorizationHeader())
	req.Header.Add("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)

	return respBody, resp.Status, resp.StatusCode
}

func partyToPatient(party *Party, patient *models.Patient) {
	patient.Id = party.Id
	patient.Dob = party.DateOfBirth
	patient.Firstname = party.FirstNames
	patient.Surname = party.LastNames
	patient.Address = party.Address.Address
	patient.Gender = party.Gender

	for _, a := range party.PartyAdditionalInfo {
		switch a.Key {
		case "rnsh.mrn":
			patient.Mrn = a.Value
		case "tumorType":
			patient.TumorType = a.Value
		case "email":
			patient.Email = a.Value
		case "phone":
			patient.Phone = a.Value
		case "surgical":
			patient.Surgical = a.Value
		}
	}
}

// patient interface (service)
func (oeps OpenEHRManager) GetAllPatients() []models.Patient {
	url := fmt.Sprintf("%sdemographics/party/query?lastNames=*&rnsh.mrn=*", oeps.BaseUrl)
	body := oeps.GetOpenEhr(url)
	var parties PartiesType
	_ = json.Unmarshal(body, &parties)
	var patients []models.Patient

	for _, p := range parties.Parties {
		var patient models.Patient
		partyToPatient(&p, &patient)
		patients = append(patients, patient)

	}

	return patients
}

func (oeps OpenEHRManager) GetPatient(id int) models.Patient {
	url := fmt.Sprintf("%sdemographics/party/%v", oeps.BaseUrl, id)
	body := oeps.GetOpenEhr(url)
	var party PartyType
	_ = json.Unmarshal(body, &party)
	var patient models.Patient
	partyToPatient(&party.Party, &patient)
	return patient
}

func (oeps OpenEHRManager) GetEhrId(mrn string) string {
	url := fmt.Sprintf("%sehr/?subjectId=%s&subjectNamespace=%s", oeps.BaseUrl, mrn, oeps.SubjectNamespace)
	body := oeps.GetOpenEhr(url)
	var ehr ehrType
	_ = json.Unmarshal(body, &ehr)
	return ehr.EhrId
}

func (oeps OpenEHRManager) CreatePatient(firstName string, surname string, gender string, dob string,
	address string, mrn string, tumorType string, surgical string, phone string, email string) models.Patient {
	additionalParty := make([]partyInfoType, 5)
	additionalParty[0].Key = "rnsh.mrn"
	additionalParty[0].Value = mrn
	additionalParty[1].Key = "tumorType"
	additionalParty[1].Value = tumorType
	additionalParty[2].Key = "surgical"
	additionalParty[2].Value = surgical
	additionalParty[3].Key = "phone"
	additionalParty[3].Value = phone
	additionalParty[4].Key = "email"
	additionalParty[4].Value = email
	newAddress := AddressType{"", address}
	party := Party{"", firstName, surname, gender, dob, newAddress, additionalParty}
	partyJson, _ := json.Marshal(party)
	url := fmt.Sprintf("%sehr/?subjectId=%s&subjectNamespace=%s", oeps.BaseUrl, mrn, oeps.SubjectNamespace)
	body, status, code := oeps.PostOpenEhr(url, nil)
	if status == "EHR-2124" && code == 400 {
		err := errors.New("A patient with this MRN already exists.")
		log.Fatal(err)
	}
	url = fmt.Sprintf("%sdemographics/party", oeps.BaseUrl)
	body, status, code = oeps.PostOpenEhr(url, partyJson)
	if status == "EHR-2124" && code == 400 {
		err := errors.New("A patient with this MRN already exists.")
		log.Fatal(err)
	}
	fmt.Println("%s", string(body))
	var partyReturn partyCreateReturn
	_ = json.Unmarshal(body, &partyReturn)
	url = partyReturn.Meta.Href
	body = oeps.GetOpenEhr(url)
	var newParty PartyType
	_ = json.Unmarshal(body, &newParty)
	var patient models.Patient
	partyToPatient(&newParty.Party, &patient)
	return patient
}
