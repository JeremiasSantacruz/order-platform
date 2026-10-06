export type ClientStatus = 'ACTIVE' | 'BLOCKED';
export type ClientSegment = 'WHOLESALE' | 'RETAIL';
export type ClientTaxRegime = 'GENERAL' | 'SIMPLIFIED' | 'EXEMPT';

export interface Client {
  id: string;
  name: string;
  market: string;
  status: ClientStatus;
  segment: ClientSegment;
  taxRegime: ClientTaxRegime;
  createdAt: Date;
}
