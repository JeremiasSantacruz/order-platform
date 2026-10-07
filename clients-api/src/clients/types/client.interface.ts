export type ClientStatus = 'ACTIVE' | 'BLOCKED';
export type ClientSegment = 'WHOLESALE' | 'RETAIL';
export type ClientTaxRegime = 'GENERAL' | 'SIMPLIFIED' | 'EXEMPT';

/**
 * Contrato JSON de la sección 5.B de Expecificaciones.md.
 * El consumidor (order-processor) espera exactamente estos campos.
 */
export interface Client {
  clientId: string;
  name: string;
  market: string;
  status: ClientStatus;
  segment: ClientSegment;
  taxRegime: ClientTaxRegime;
}
