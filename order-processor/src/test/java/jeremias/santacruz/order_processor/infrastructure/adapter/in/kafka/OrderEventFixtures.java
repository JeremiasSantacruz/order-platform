package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

/**
 * Payloads de prueba del contrato {@code orders.created.v1} (sección 5.A de
 * Especificaciones.md).
 */
final class OrderEventFixtures {

    private OrderEventFixtures() {
    }

    /**
     * Payload válido completo, incluido el campo desconocido {@code occurredAt}: el lector lo
     * tolera y la validación se ocupa solo de los campos conocidos del contrato.
     */
    static String validPayload() {
        return """
                {
                  "eventId": "01J8ZP6M5E4RH0K7Y2N9A3TQWX",
                  "eventVersion": 1,
                  "occurredAt": "2026-09-18T15:42:10Z",
                  "orderId": "ORD-MX-000147",
                  "market": "MX",
                  "currency": "MXN",
                  "clientId": "CLI-99821",
                  "channel": "C1",
                  "items": [
                    {"productId": "PRD-001", "quantity": 24, "unitPrice": 35.5},
                    {"productId": "PRD-008", "quantity": 12, "unitPrice": 82.0}
                  ]
                }
                """;
    }

    /** Mismo payload sin el campo opcional {@code eventVersion}: se asume la versión 1. */
    static String payloadWithoutEventVersion() {
        return validPayload().replace("  \"eventVersion\": 1,\n", "");
    }

    /** Regla 2: {@code items} vacío. */
    static String emptyItemsPayload() {
        return validPayload().replace(
                """
                  "items": [
                    {"productId": "PRD-001", "quantity": 24, "unitPrice": 35.5},
                    {"productId": "PRD-008", "quantity": 12, "unitPrice": 82.0}
                  ]
                """,
                "  \"items\": []");
    }

    /** Regla 1: falta el campo obligatorio {@code orderId}. */
    static String payloadWithoutOrderId() {
        return validPayload().replace("  \"orderId\": \"ORD-MX-000147\",\n", "");
    }

    /** Regla 4: {@code quantity} decimal se rechaza al deserializar (debe ser entero). */
    static String fractionalQuantityPayload() {
        return validPayload().replace("\"quantity\": 24", "\"quantity\": 24.5");
    }

    /** Payload que no es JSON válido. */
    static String malformedPayload() {
        return "{esto-no-es-json";
    }
}
