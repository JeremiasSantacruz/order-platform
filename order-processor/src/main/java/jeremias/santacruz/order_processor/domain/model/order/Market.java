package jeremias.santacruz.order_processor.domain.model.order;


public enum Market {
    MX(Currency.MXN),
    CO(Currency.COP),
    PE(Currency.PEN);

    private final Currency defaultCurrency;

    Market(Currency defaultCurrency) {
        this.defaultCurrency = defaultCurrency;
    }

    public Currency getDefaultCurrency() {
        return defaultCurrency;
    }

    public boolean supportsCurrency(Currency currency) {
        return this.defaultCurrency == currency;
    }
}

