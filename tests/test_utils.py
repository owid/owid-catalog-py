from owid.catalog.utils import underscore


def test_underscore():

    assert underscore("Urban population") == "urban_population"
    assert (
        underscore("Urban population (% of total population)")
        == "urban_population__pct_of_total_population"
    )
    assert (
        underscore("Women's share of population ages 15+ living with HIV (%)")
        == "womens_share_of_population_ages_15plus_living_with_hiv__pct"
    )
    assert (
        underscore(
            "Water productivity, total (constant 2010 US$ GDP per cubic meter of total freshwater withdrawal)"
        )
        == "water_productivity__total__constant_2010_usd_gdp_per_cubic_meter_of_total_freshwater_withdrawal"
    )
    assert (
        underscore("Agricultural machinery, tractors per 100 sq. km of arable land")
        == "agricultural_machinery__tractors_per_100_sq__km_of_arable_land"
    )
    assert (
        underscore("GDP per capita, PPP (current international $)")
        == "gdp_per_capita__ppp__current_international_dollar"
    )
    assert (
        underscore("Automated teller machines (ATMs) (per 100,000 adults)")
        == "automated_teller_machines__atms__per_100_000_adults"
    )
