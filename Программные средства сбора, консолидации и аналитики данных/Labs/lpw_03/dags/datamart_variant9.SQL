-- Grain: one canonical country in one report year.
-- gdp_per_capita and social_support are score components, not GDP in USD.
CREATE OR REPLACE VIEW happiness_datamart AS
SELECT
    country,
    region,
    year,
    happiness_score,
    gdp_per_capita,
    social_support,
    make_date(year, 1, 1) AS report_date
FROM stg_happiness;

COMMENT ON VIEW happiness_datamart IS
    'Variant 09: World Happiness 2015-2019; country/report-year observations';
COMMENT ON COLUMN happiness_datamart.gdp_per_capita IS
    'GDP contribution to happiness score, points; NOT monetary GDP per capita';
COMMENT ON COLUMN happiness_datamart.social_support IS
    'Family/Social support contribution to happiness score, points';
