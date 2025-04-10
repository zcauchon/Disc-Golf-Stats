import pandas as pd
from requests import get
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer

from .parser.extract_event_info import event_info_extractor
from .orchestration.partitions import daily_partitions

from .project import dbt_project

from dagster import asset, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource

@asset(
    group_name="web",
    compute_kind="python",
    partitions_def=daily_partitions,
    pool="stg"
)
def event_requests_stg(context: AssetExecutionContext) -> None:
    """
        Find recently update events using pdga tour search
    """

    engine = create_engine("postgresql+psycopg2://edwetl:edwetl@localhost:5432/pdga")
    
    start_date = context.partition_key
    end_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
    end_date = end_date.strftime('%Y-%m-%d')
    url_base = 'https://www.pdga.com'
    search_url = f'/tour/search?date_filter[min][date]={start_date}&date_filter[max][date]={end_date}&Country[]=United+States&Tier[]=A&Tier[]=A%2FB&Tier[]=A%2FC&Tier[]=B&Tier[]=B%2FA&Tier[]=B%2FC&Tier[]=C&Tier[]=C%2FA&Tier[]=C%2FB'
    results = []

    while True:
        # make request to find recent events
        page = get(f"{url_base}{search_url}")
        soup = BeautifulSoup(page.content, "html.parser")
        # parse results 
        for event in soup.find_all('td', attrs={'class':'views-field-OfficialName'}):
            event_url = f'{url_base}{event.a["href"]}'
            # add event to results
            results.append([event_url.split('/')[-1], date.today(), 1])
        # check if there are paged results
        next_page = soup.find("li", attrs={"class":"pager-next"})
        if next_page is None:
            # no more results, break out of loop
            break
        print('More results available...')
        #update url to next page and make another request
        search_url = next_page.a["href"]
    
    # once everything has been identified
    # create df of new events and save to stg
    df = pd.DataFrame(columns=["event_id", "scrape_date", "status"], data=results)
    df.drop_duplicates(inplace=True)
    with engine.begin() as con:
        nrows = 0
        if len(df) > 0:
            nrows = df.to_sql('event_requests', schema="pdga_stg", if_exists="replace", con=con, index=False, dtype={"event_id": Integer})
    print(f'Loaded {nrows} rows for processing')

@asset(
    deps=[event_requests_stg],
    group_name="web",
    compute_kind="python",
    pool="stg"
)
def event_details_stg() -> pd.DataFrame:
    """
        Get event details for identified events
    """

    engine = create_engine("postgresql+psycopg2://edwetl:edwetl@localhost:5432/pdga")

    with engine.begin() as con:
        _sql = """select distinct event_id 
                from pdga_stg.event_requests s
                where status = 1
                union
                select event_id
                from pdga.event_requests
                where status = 2
                and scrape_date < CURRENT_DATE-5
                except
                select event_id 
                from pdga.event_requests
                where status = 4;"""
        pending_events = con.execute(text(_sql))
        results = pd.DataFrame()
        for event in pending_events:
            status, event_info = event_info_extractor(event[0])
            # would probably be better to do this all at once
            con.execute(text(f"update pdga_stg.event_requests set status = {status} where event_id = {event[0]}"))
            if event_info is not None:
                results = pd.concat([results, event_info])
        con.execute(text("truncate pdga_stg.event_details"))
        if len(results) > 0:
            print(f"Writing {len(results)} new events to event_details")
            results.to_sql("event_details", con=con, schema="pdga_stg", index=False, if_exists="replace")
    return results


### DBT ###
@dbt_assets(
    manifest=dbt_project.manifest_path,
    pool="stg"
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_build_invocation = dbt.cli(["build"], context=context)
    yield from dbt_build_invocation.stream()