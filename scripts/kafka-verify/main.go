// kafka-verify publica eventos orders.created.v1 en Kafka y verifica el resultado
// esperado en MongoDB (orders_db.orders) y en el evento de salida orders.processed.v1.
//
// Uso: go run . -cases all
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const (
	topicIn   = "orders.created.v1"
	topicOut  = "orders.processed.v1"
	mongoDB   = "orders_db"
	mongoColl = "orders"
)

var (
	brokers   = flag.String("kafka", "localhost:9092", "bootstrap servers de Kafka")
	mongoURI  = flag.String("mongo", "mongodb://product:secret@localhost:27017/?authSource=admin", "URI de MongoDB")
	casesFlag = flag.String("cases", "all", "casos a ejecutar: all | happy,blocked,notfound,idempotent")
	timeout   = flag.Duration("timeout", 45*time.Second, "timeout por caso")
	prefix    = flag.String("prefix", "ORD-GO", "prefijo para los orderIds generados")
	skipOut   = flag.Bool("skip-output", false, "no verificar el evento orders.processed.v1")
)

// --- colores -----------------------------------------------------------------

var (
	red, green, yellow, reset string
)

func init() {
	if fi, err := os.Stdout.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		red, green, yellow, reset = "\033[31m", "\033[32m", "\033[33m", "\033[0m"
	}
}

func ok(format string, a ...any) {
	fmt.Printf("   %s✔%s %s\n", green, reset, fmt.Sprintf(format, a...))
}
func ko(format string, a ...any) {
	fmt.Printf("   %s✘%s %s\n", red, reset, fmt.Sprintf(format, a...))
}
func info(format string, a ...any) {
	fmt.Printf("   %s•%s %s\n", yellow, reset, fmt.Sprintf(format, a...))
}

// --- contrato de entrada (sección 5.A) ---------------------------------------

type item struct {
	ProductID string  `json:"productId"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unitPrice"`
}

type createdEvent struct {
	EventID      string `json:"eventId"`
	EventVersion int    `json:"eventVersion"`
	OccurredAt   string `json:"occurredAt"`
	OrderID      string `json:"orderId"`
	Market       string `json:"market"`
	Currency     string `json:"currency"`
	ClientID     string `json:"clientId"`
	Channel      string `json:"channel"`
	Items        []item `json:"items"`
}

// --- contrato de salida (sección 5.D) ----------------------------------------

type outTotals struct {
	GrossSubtotal json.Number `json:"grossSubtotal"`
	Discount      json.Number `json:"discount"`
	NetSubtotal   json.Number `json:"netSubtotal"`
	Tax           json.Number `json:"tax"`
	GrandTotal    json.Number `json:"grandTotal"`
}

type processedEvent struct {
	SourceEventID string    `json:"sourceEventId"`
	OrderID       string    `json:"orderId"`
	Status        string    `json:"status"`
	Reason        *string   `json:"reason"`
	Totals        outTotals `json:"totals"`
}

// --- documento Mongo ----------------------------------------------------------
//
// Spring Data persiste el @Id del record como _id (no hay campo "orderId") y
// OrderTotals nombra los importes taxAmount/total (el contrato 5.D usa
// tax/grandTotal solo en el evento de salida).

type orderDoc struct {
	OrderID         string         `bson:"_id"`
	EventID         string         `bson:"eventId"`
	EventVersion    int64          `bson:"eventVersion"`
	Status          string         `bson:"status"`
	RejectionReason string         `bson:"rejectionReason"`
	Totals          map[string]any `bson:"totals"`
}

// nombre del campo de totals en Mongo para cada nombre del contrato.
var mongoAmountField = map[string]string{
	"grossSubtotal": "grossSubtotal",
	"discount":      "discount",
	"netSubtotal":   "netSubtotal",
	"tax":           "taxAmount",
	"grandTotal":    "total",
}

// --- casos --------------------------------------------------------------------

type expectation struct {
	status        string
	reasonMatches string
	// amounts: campo de totals (Mongo) -> importe esperado como texto.
	amounts map[string]string
}

type scenario struct {
	name      string
	desc      string
	payload   createdEvent
	expect    expectation
	republish bool // idempotencia: re-entrega del mismo evento
}

func newScenario(name, desc string, mk func(run string) createdEvent, exp expectation, republish bool) scenario {
	// Sufijo único por caso: evita que dos escenarios compartan orderId/eventId
	// (el procesador deduplica por eventId, sección 7.1).
	run := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	return scenario{name: name, desc: desc, payload: mk(run), expect: exp, republish: republish}
}

func scenarios() map[string]scenario {
	happyPayload := func(run string) createdEvent {
		return createdEvent{
			EventID: "EVT-GO-HAPPY-" + run, EventVersion: 1,
			OccurredAt: time.Now().UTC().Format(time.RFC3339),
			OrderID:    *prefix + "-MX-000147-" + run, Market: "MX", Currency: "MXN",
			ClientID: "CLI-99821", Channel: "C1",
			Items: []item{{"PRD-001", 24, 35.5}, {"PRD-008", 12, 82.0}},
		}
	}
	return map[string]scenario{
		"happy": newScenario("happy", "camino feliz 5.A→5.D: APPROVED con totales exactos", happyPayload,
			expectation{status: "APPROVED", amounts: map[string]string{
				"grossSubtotal": "1836.00", "discount": "25.56", "netSubtotal": "1810.44",
				"tax": "289.67", "grandTotal": "2100.11",
			}}, false),

		"blocked": newScenario("blocked", "cliente BLOCKED (CLI-0003) → REJECTED / CLIENT_INACTIVE",
			func(run string) createdEvent {
				return createdEvent{
					EventID: "EVT-GO-BLOCKED-" + run, EventVersion: 1,
					OccurredAt: time.Now().UTC().Format(time.RFC3339),
					OrderID:    *prefix + "-BLOCKED-" + run, Market: "MX", Currency: "MXN",
					ClientID: "CLI-0003", Channel: "C1",
					Items: []item{{"PRD-001", 1, 10}},
				}
			},
			expectation{status: "REJECTED", reasonMatches: "CLIENT_INACTIVE"}, false),

		"notfound": newScenario("notfound", "producto inexistente → REJECTED / PRODUCT_NOT_FOUND",
			func(run string) createdEvent {
				return createdEvent{
					EventID: "EVT-GO-PNF-" + run, EventVersion: 1,
					OccurredAt: time.Now().UTC().Format(time.RFC3339),
					OrderID:    *prefix + "-PNF-" + run, Market: "MX", Currency: "MXN",
					ClientID: "CLI-99821", Channel: "C1",
					Items: []item{{"PRD-99999", 1, 10}},
				}
			},
			expectation{status: "REJECTED", reasonMatches: "PRODUCT_NOT_FOUND"}, false),

		"idempotent": newScenario("idempotent", "re-entrega del mismo eventId (7.1) → 1 solo documento", happyPayload,
			expectation{status: "APPROVED", amounts: map[string]string{
				"grossSubtotal": "1836.00", "discount": "25.56", "netSubtotal": "1810.44",
				"tax": "289.67", "grandTotal": "2100.11",
			}}, true),
	}
}

// --- main ---------------------------------------------------------------------

func main() {
	flag.Parse()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(*brokers),
		Topic:                  topicIn,
		RequiredAcks:           kafka.RequireAll,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := mongo.Connect(options.Client().ApplyURI(*mongoURI))
	if err != nil {
		fatal("no se pudo conectar a MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		fatal("MongoDB no responde en %s: %v", *mongoURI, err)
	}
	cancel()

	coll := client.Database(mongoDB).Collection(mongoColl)

	all := scenarios()
	order := []string{"happy", "blocked", "notfound", "idempotent"}
	want := strings.ToLower(*casesFlag)
	if want != "all" {
		order = nil
		for _, c := range strings.Split(want, ",") {
			c = strings.TrimSpace(c)
			if _, found := all[c]; !found {
				fatal("caso desconocido: %q (disponibles: all, happy, blocked, notfound, idempotent)", c)
			}
			order = append(order, c)
		}
	}

	fmt.Printf("\nkafka-verify — kafka=%s mongo=%s/%s\n\n", *brokers, *mongoURI, mongoDB)

	passed, failed := 0, 0
	for i, key := range order {
		sc := all[key]
		fmt.Printf("%d) %s — %s\n", i+1, sc.name, sc.desc)
		if err := runCase(writer, coll, sc); err != nil {
			ko("%v", err)
			failed++
		} else {
			passed++
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("─", 70))
	fmt.Printf("  %s%d OK%s, %s%d FAIL%s\n\n", green, passed, reset, red, failed, reset)
	if failed > 0 {
		os.Exit(1)
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, red+"✘ "+format+reset+"\n", a...)
	os.Exit(2)
}

// --- ejecución de un caso -----------------------------------------------------

func runCase(w *kafka.Writer, coll *mongo.Collection, sc scenario) error {
	var fails []string
	check := func(cond bool, format string, a ...any) {
		if cond {
			ok(format, a...)
		} else {
			ko(format, a...)
			fails = append(fails, fmt.Sprintf(format, a...))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	info("publicando %s en %s (eventId=%s)", sc.payload.OrderID, topicIn, sc.payload.EventID)
	if err := publish(ctx, w, sc.payload); err != nil {
		return fmt.Errorf("no se pudo publicar el evento: %w", err)
	}

	doc, err := waitForTerminal(ctx, coll, sc.payload.OrderID)
	if err != nil {
		return fmt.Errorf("espera en MongoDB: %w", err)
	}

	check(doc.Status == sc.expect.status, "Mongo status=%s (esperado %s)", doc.Status, sc.expect.status)
	check(doc.EventID == sc.payload.EventID, "Mongo eventId=%s (esperado %s)", doc.EventID, sc.payload.EventID)
	if sc.expect.reasonMatches != "" {
		check(strings.Contains(doc.RejectionReason, sc.expect.reasonMatches),
			"Mongo rejectionReason contiene %q (got %q)", sc.expect.reasonMatches, doc.RejectionReason)
	}
	for field, want := range sc.expect.amounts {
		got, found := dbAmount(doc.Totals, field)
		check(found && sameDecimal(got, want), "Mongo totals.%s=%s (esperado %s)", field, got, want)
	}

	n, err := coll.CountDocuments(ctx, bson.M{"_id": sc.payload.OrderID})
	if err != nil {
		return fmt.Errorf("countDocuments: %w", err)
	}
	check(n == 1, "1 solo documento en Mongo (count=%d)", n)

	if sc.republish {
		info("re-entregando el mismo evento (idempotencia 7.1)…")
		if err := publish(ctx, w, sc.payload); err != nil {
			return fmt.Errorf("re-publicación: %w", err)
		}
		time.Sleep(6 * time.Second)
		n, err := coll.CountDocuments(ctx, bson.M{"_id": sc.payload.OrderID})
		if err != nil {
			return fmt.Errorf("countDocuments: %w", err)
		}
		check(n == 1, "sigue habiendo 1 solo documento tras la re-entrega (count=%d)", n)
		var doc2 orderDoc
		if err := coll.FindOne(ctx, bson.M{"_id": sc.payload.OrderID}).Decode(&doc2); err != nil {
			return fmt.Errorf("re-lectura: %w", err)
		}
		check(doc2.Status == sc.expect.status, "status intacto tras re-entrega (%s)", doc2.Status)
		for field, want := range sc.expect.amounts {
			got, found := dbAmount(doc2.Totals, field)
			check(found && sameDecimal(got, want), "totals.%s intacto=%s (esperado %s)", field, got, want)
		}
	}

	if !*skipOut {
		out, err := waitForOutput(ctx, sc.payload.OrderID)
		if err != nil {
			check(false, "evento %s: %v", topicOut, err)
		} else {
			check(out.Status == sc.expect.status, "%s status=%s (esperado %s)", topicOut, out.Status, sc.expect.status)
			check(out.SourceEventID == sc.payload.EventID, "%s sourceEventId=%s", topicOut, out.SourceEventID)
			if sc.expect.reasonMatches != "" {
				reason := ""
				if out.Reason != nil {
					reason = *out.Reason
				}
				check(strings.Contains(reason, sc.expect.reasonMatches),
					"%s reason contiene %q (got %q)", topicOut, sc.expect.reasonMatches, reason)
			}
			for field, want := range sc.expect.amounts {
				got := outField(&out.Totals, field)
				check(sameNumber(got, want), "%s totals.%s=%s (esperado %s)", topicOut, field, got, want)
			}
		}
	}

	if len(fails) > 0 {
		return fmt.Errorf("%d verificación(es) fallida(s)", len(fails))
	}
	return nil
}

// --- Kafka --------------------------------------------------------------------

func publish(ctx context.Context, w *kafka.Writer, ev createdEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(ev.OrderID), Value: payload})
}

// waitForOutput lee orders.processed.v1 desde el inicio hasta encontrar el orderId.
func waitForOutput(ctx context.Context, orderID string) (*processedEvent, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*brokers},
		Topic:       topicOut,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("timeout leyendo %s para %s: %w", topicOut, orderID, err)
		}
		var ev processedEvent
		d := json.NewDecoder(bytes.NewReader(m.Value))
		d.UseNumber()
		if err := d.Decode(&ev); err != nil {
			continue
		}
		if ev.OrderID == orderID {
			return &ev, nil
		}
	}
}

// --- MongoDB ------------------------------------------------------------------

func waitForTerminal(ctx context.Context, coll *mongo.Collection, orderID string) (*orderDoc, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		var doc orderDoc
		err := coll.FindOne(ctx, bson.M{"_id": orderID}).Decode(&doc)
		if err == nil && doc.Status != "" {
			return &doc, nil
		}
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		if err == nil {
			last = "status vacío"
		} else {
			last = "documento aún no existe"
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout esperando estado terminal para %s (%s)", orderID, last)
		case <-tick.C:
		}
	}
}

// --- helpers ------------------------------------------------------------------

// dbAmount extrae un importe de totals (Mongo) como texto, sea string, número
// o Decimal128. El campo se busca con el nombre del contrato (ej. "tax") y se
// traduce al nombre real del documento (ej. "taxAmount").
func dbAmount(totals map[string]any, contractField string) (string, bool) {
	if totals == nil {
		return "", false
	}
	v, ok := totals[mongoAmountField[contractField]]
	if !ok || v == nil {
		return "", false
	}
	switch x := v.(type) {
	case string:
		return x, true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case bson.Decimal128:
		return x.String(), true
	default:
		return fmt.Sprintf("%v", x), true
	}
}

func outField(t *outTotals, field string) json.Number {
	switch field {
	case "grossSubtotal":
		return t.GrossSubtotal
	case "discount":
		return t.Discount
	case "netSubtotal":
		return t.NetSubtotal
	case "tax":
		return t.Tax
	case "grandTotal":
		return t.GrandTotal
	}
	return json.Number("")
}

func sameNumber(got json.Number, want string) bool {
	return sameDecimal(got.String(), want)
}

// sameDecimal compara importes como racionales exactos (tolerante a formato/escala).
func sameDecimal(a, b string) bool {
	ra, ok1 := parseRat(a)
	rb, ok2 := parseRat(b)
	if !ok1 || !ok2 {
		return false
	}
	return ra.Cmp(rb) == 0
}

func parseRat(s string) (*big.Rat, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if r, ok := new(big.Rat).SetString(s); ok {
		return r, true
	}
	// Notación exponencial (1.836E+3): fallback por float64.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, false
	}
	r := new(big.Rat).SetFloat64(f)
	if r == nil {
		return nil, false
	}
	return r, true
}
