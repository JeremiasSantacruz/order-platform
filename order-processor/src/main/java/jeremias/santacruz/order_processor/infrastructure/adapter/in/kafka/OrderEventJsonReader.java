package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Lector del contrato JSON {@code orders.created.v1}.
 *
 * <p>Usa un {@link ObjectMapper} propio y estricto para no acoplarse a la configuración general
 * de la aplicación:</p>
 * <ul>
 *   <li>Regla 4: {@code quantity} debe ser entero, por lo que se desactiva
 *       {@code ACCEPT_FLOAT_AS_INT} y un {@code 24.5} se rechaza en lugar de truncarse a
 *       {@code 24}.</li>
 *   <li>Lectura tolerante a campos nuevos ({@code FAIL_ON_UNKNOWN_PROPERTIES} desactivado):
 *       un productor puede evolucionar el payload sin romper este consumidor; lo que se valida
 *       es sobre los campos conocidos del contrato.</li>
 * </ul>
 */
@Component
public class OrderEventJsonReader {

    private final ObjectMapper mapper = JsonMapper.builder()
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    /**
     * Deserializa el payload al DTO de contrato.
     *
     * @param payload Mensaje crudo consumido del tópico
     * @return evento de entrada ya tipado
     * @throws ContractViolationException con categoría {@code DESERIALIZATION} si el payload
     *         no es JSON válido ni ajustado al esquema
     */
    public OrderCreatedEventDto readEvent(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new ContractViolationException(ContractViolationException.Category.DESERIALIZATION,
                    "payload vacío");
        }
        try {
            OrderCreatedEventDto event = mapper.readValue(payload, OrderCreatedEventDto.class);
            if (event == null) {
                throw new ContractViolationException(ContractViolationException.Category.DESERIALIZATION,
                        "payload JSON nulo");
            }
            return event;
        }
        catch (JsonProcessingException | IllegalArgumentException ex) {
            throw new ContractViolationException(ContractViolationException.Category.DESERIALIZATION,
                    "JSON inválido: " + describe(ex));
        }
    }

    /**
     * Lee de forma no bloqueante un campo de texto del payload, para conservar el identificador
     * del mensaje en los headers de la DLT aunque no se pueda deserializar completo.
     *
     * @param payload Mensaje crudo
     * @param field   Nombre del campo
     * @return valor del campo o vacío si no existe o el JSON es irrelevante
     */
    public Optional<String> readTextField(String payload, String field) {
        if (payload == null || payload.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode root = mapper.readTree(payload);
            JsonNode value = root == null ? null : root.get(field);
            return (value != null && value.isTextual() && !value.asText().isBlank())
                    ? Optional.of(value.asText())
                    : Optional.empty();
        }
        catch (JsonProcessingException ex) {
            return Optional.empty();
        }
    }

    private static String describe(Exception ex) {
        if (ex instanceof JsonProcessingException jsonException) {
            return jsonException.getOriginalMessage();
        }
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }
}
