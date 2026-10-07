package jeremias.santacruz.order_processor.domain.port.out;

import jeremias.santacruz.order_processor.domain.model.client.Client;

import java.util.Optional;

public interface ClientsClientPort {
    /**
     * Consulta la información de un cliente por su ID.
     *
     * @param clientId Identificador único del cliente
     * @return Optional con el Cliente de dominio, o vacio si no existe (404)
     * @throws ExternalServiceException si ocurre un fallo técnico/transitorio no recuperable
     */
    Optional<Client> getClient(String clientId);
}
