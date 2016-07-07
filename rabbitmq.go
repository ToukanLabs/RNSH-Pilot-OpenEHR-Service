package main

import (
	"fmt"
	"log"

	"encoding/json"

	"github.com/FiviumAustralia/RNSH-Pilot-OpenEHR-Service/openehr"
	"github.com/FiviumAustralia/RNSH-Pilot-Server-Go/models"
	"github.com/FiviumAustralia/RNSH-Pilot-Server-Go/serviceshelper"
	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://go-openehr-service:go-openehr-service@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	log.Println("Connected to RabbitMQ.")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"patient_queue", // name
		false,           // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {

			requestBody := serviceshelper.RPCClientRequest{}
			err := json.Unmarshal(d.Body, &requestBody)
			failOnError(err, "Failed to unmarshall body.")

			log.Printf("[ START ] Handling %s", requestBody.Method)

			oeps := openehr.OpenEHRManager{
				BaseUrl:          "https://ehrscape.code-4-health.org/rest/v1/",
				SubjectNamespace: "rnsh.mrn",
				Username:         "rnshpilot_c4h",
				Password:         "lIsombRI",
			}

			var responseBody interface{}
			switch requestBody.Method {
			case serviceshelper.RPC_METHOD_GET_ALL_PATIENTS:
				responseBody = oeps.GetAllPatients()
			case serviceshelper.RPC_METHOD_GET_PATIENT:
				rpgp := serviceshelper.RPCParamsGetPatient{}
				err := json.Unmarshal([]byte(*requestBody.Params), &rpgp)
				failOnError(err, "Unable to unmarshall GetPatients params")
				responseBody = oeps.GetPatient(rpgp.PatientId)
			case serviceshelper.RPC_METHOD_GET_EHR_ID:
				rpgei := serviceshelper.RPCParamsGetEhrId{}
				err := json.Unmarshal([]byte(*requestBody.Params), &rpgei)
				failOnError(err, "Unable to unmarshall GetEhrId params")
				ehrId := oeps.GetEhrId(rpgei.MRN)
				responseBody = serviceshelper.RPCResultGetEhrId{
					EhrId: ehrId,
				}
			case serviceshelper.RPC_METHOD_CREATE_PATIENT:
				patient := models.Patient{}
				err := json.Unmarshal([]byte(*requestBody.Params), &patient)
				failOnError(err, "Unable to unmarshall CreatePatient params")
				responseBody = oeps.CreatePatient(patient.Firstname, patient.Surname, patient.Gender, patient.Dob, patient.Address, patient.Mrn, patient.TumorType, patient.Surgical, patient.Phone, patient.Email)
			}

			responseBodyByte, err := json.Marshal(responseBody)
			failOnError(err, "Failed to marshall response body.")

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          responseBodyByte,
				})
			failOnError(err, "Failed to publish a message")

			log.Printf("[  END  ] Handling %s", requestBody.Method)

			d.Ack(false)
		}
	}()

	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
