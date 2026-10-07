package jeremias.santacruz.order_processor.infrastructure.adapter.in.kafka;

/**
 * Error no recuperable del contrato de entrada: el mensaje nunca podrá procesarse correctamente,
 * por lo que se clasifica como <em>no reintentable</em> y va directo a la DLT
 * (sección 7 de Especificaciones.md).
 *
 * <p>El motivo se conserva en {@code summaryCause} para publicarlo como header de la DLT.</p>
 */
public class ContractViolationException extends RuntimeException {

    /** Categoría técnica del fallo, expuesta como header {@code errorCategory} de la DLT. */
    public enum Category {
        /** El payload no es JSON válido o no se ajusta al esquema del contrato. */
        DESERIALIZATION,
        /** JSON válido que incumple una regla de validación de entrada. */
        CONTRACT_VIOLATION
    }

    private final Category category;
    private final String summaryCause;

    public ContractViolationException(Category category, String summaryCause) {
        super(summaryCause);
        this.category = category;
        this.summaryCause = summaryCause;
    }

    public Category getCategory() {
        return category;
    }

    public String getSummaryCause() {
        return summaryCause;
    }

    /**
     * Busca la causa entre las excepciones anidadas: el contenedor de Kafka envuelve las
     * excepciones del listener en {@code ListenerExecutionFailedException} y el manejador de
     * errores puede añadir más capas durante los reintentos.
     *
     * @param throwable Excepción recibida por el recoverer
     * @return la primera {@link ContractViolationException} de la cadena, o {@code null}
     */
    public static ContractViolationException findIn(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ContractViolationException contractViolation) {
                return contractViolation;
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return null;
    }
}