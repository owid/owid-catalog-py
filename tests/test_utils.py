import pandas as pd
from owid.catalog import Table
from owid.catalog.utils import underscore, underscore_table


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
    assert (
        underscore(
            "Political regimes - OWID based on Boix et al. (2013), V-Dem (v12), and Lührmann et al. (2018)"
        )
        == "political_regimes__owid_based_on_boix_et_al__2013__v_dem__v12__and_luhrmann_et_al__2018"
    )
    assert (
        underscore("Adjusted savings: particulate emission damage (current US$)")
        == "adjusted_savings__particulate_emission_damage__current_usd"
    )
    assert (
        underscore(
            "Benefit incidence of unemployment benefits and ALMP to poorest quintile (% of total U/ALMP benefits)"
        )
        == "benefit_incidence_of_unemployment_benefits_and_almp_to_poorest_quintile__pct_of_total_u_almp_benefits"
    )
    assert (
        underscore(
            "Business extent of disclosure index (0=less disclosure to 10=more disclosure)"
        )
        == "business_extent_of_disclosure_index__0_less_disclosure_to_10_more_disclosure"
    )
    assert (
        underscore("Firms that spend on R&D (% of firms)")
        == "firms_that_spend_on_r_and_d__pct_of_firms"
    )


def test_underscore_table():
    df = pd.DataFrame({"A": [1, 2, 3]})
    df.index.names = ["I"]

    t = Table(df)
    t["A"].metadata.description = "column A"

    tt = underscore_table(t)
    assert tt.columns == ["a"]
    assert tt.index.names == ["i"]
    assert tt["a"].metadata.description == "column A"
