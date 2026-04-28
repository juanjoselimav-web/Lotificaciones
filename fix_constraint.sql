-- Drop old constraint and add new one including cuenta_contrapartida
ALTER TABLE flujos_efectivo DROP CONSTRAINT IF EXISTS flujos_efectivo_sociedad_belnr_gjahr_linea_key;
ALTER TABLE flujos_efectivo ADD CONSTRAINT flujos_efectivo_sociedad_belnr_gjahr_linea_cta_key 
    UNIQUE (sociedad, belnr, gjahr, linea, cuenta_contrapartida);
