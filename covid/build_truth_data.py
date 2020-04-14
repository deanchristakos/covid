#!/usr/bin/python3
from astor_globals import *
from astor_square_utils import *
import urllib
import datetime
from datetime import *
import numpy

pct_require_bed = .0125
days_to_hospitalization = 12.1
days_to_death = 23.6
fatality_rate = .007
median_hospital_stay = 15.0
prev_double_rate_days = 5.0
double_rate_days = 5.0
stay_at_home_pct = 1

def get_ground_truth(state):
    url = 'https://covidtracking.com/api/states/daily'
    # dbconnection = getDBConnection(cfg_dir + '/local-covid.ini')
    # cursor = dbconnection.cursor()

    columns = ['date', 'state', 'positive', 'negative', 'pending', 'hospitalized', 'death',
               'total', 'totalTestResults', 'fips', 'deathIncrease', 'hospitalizedIncrease', 'negativeIncrease',
               'positiveIncrease', 'totalTestResultsIncrease']

    truth_data = {}
    with urllib.request.urlopen(url) as url_data:
        data = json.loads(url_data.read().decode())
        for entry in data:
            if entry['state'] != state:
                continue
            # print(entry)
            row_array = []
            for c in columns:
                try:
                    if c == 'date':
                        # datestr = datetime.datetime.strftime('%Y%m%d')
                        row_array.append(str(entry[c]))
                    else:
                        row_array.append(entry[c])
                except KeyError:
                    row_array.append(None)
            # row = tuple(row_array)
            key = entry['state'] + str(entry['date'])
            truth_data[key] = entry

    return truth_data


def convert_truth_data_to_timeseries(truth_data):

    hospitalizations = []
    deaths = []
    positives = []
    for k in sorted(truth_data.keys()):
        dt = k[2:]
        hospitalizations.append( {'date':dt, 'val': truth_data[k]['hospitalizedIncrease'] } )
        deaths.append( { 'date':dt, 'val': truth_data[k]['deathIncrease'] })
        positives.append( { 'date':dt, 'val': truth_data[k]['positiveIncrease'] } )

    return { 'actual_hospitalizations': hospitalizations, 'actual_deaths': deaths, 'actual_positives': positives }


def create_model(state, start_pop, r0, start_date, starting_infections, interval, weather_adj_val, r_override, r_override_date):
    num_removed = 0
    query = "SELECT * FROM state_stats WHERE state_abbrev = %s"
    dbconnection = getDBConnection(cfg_dir + env + '-covid.ini')
    cursor = dbconnection.cursor()
    cursor.execute(query, (state,))
    row = cursor.fetchone()
    if row is None:
        return None
    population = row[5]
    stay_at_home_pct = row[6]
    stay_at_home_date = row[7]
    business_closed_date = row[8]
    schools_closed_date = row[9]
    pop_density_adj = row[12]
    start_date_state = row[13]
    spring_arrives = row[15]
    num_hospitals = row[16]
    staffed_beds = row[17]
    pct_unusable_beds = row[22]
    pct_require_bed = row[23]
    days_to_hospitalization = row[24]
    days_to_death = row[25]
    fatality_rate = row[26]
    median_hospital_stay = row[27]

    us_population = 327200000
    yearly_deaths = 2813503
    deaths_per_day = yearly_deaths/365

    yearly_births = 3791712
    births_per_day = yearly_births/365


    current_population = population
    total_infected = starting_infections

    schools_closed_adj = 0
    business_closed_adj = 0

    # do 100 data points
    current_date_obj = start_date
    new_infections = -1
    base_rt = r0 + pop_density_adj
    rt = base_rt
    total_hospitalizations = 0
    total_infected_living = 0
    total_deaths = 0
    removed_epop = 0
    stay_at_home_adj = 0
    weather_adj = 0
    new_deaths = 0

    cases = {}
    total_cases = {}
    deaths = {}
    total_deaths_by_date = {}
    hospitalizations = {}
    total_hospitalization_by_date = {}
    new_hospitalizations_by_date = {}
    total_infected_living_by_date = {}

    current_date = current_date_obj.strftime('%Y%m%d')
    cases[current_date] = starting_infections
    new_infections = starting_infections
    total_cases[current_date] = starting_infections
    total_deaths_by_date[current_date] = 0
    total_hospitalization_by_date[current_date] = 0
    total_infected_living_by_date[current_date] = 0
    deaths[current_date] = 0
    total_deaths_by_date[current_date] = 0

    for d in range(0,30):
        for v in range(1,interval):
            btw_date_obj = current_date_obj + timedelta(days=v)
            btw_date = btw_date_obj.strftime('%Y%m%d')
            cases[btw_date] = cases[current_date]
            #deaths[btw_date] = deaths[current_date]
            #total_deaths_by_date[btw_date] = total_deaths_by_date[current_date]
            total_cases[btw_date] = total_cases[current_date]
            #total_hospitalization_by_date[btw_date] = total_hospitalization_by_date[current_date]
            #total_infected_living_by_date[btw_date] = total_infected_living_by_date[current_date]

        current_date_obj = current_date_obj + timedelta(days=interval)
        current_date = current_date_obj.strftime('%Y%m%d')

        non_covid_deaths = deaths_per_day*interval*current_population/us_population
        births = births_per_day*interval*current_population/us_population

        num_removed = removed_epop
        removed_epop = total_infected - total_deaths

        current_population = current_population + births - non_covid_deaths - new_deaths
        susceptible_pop = max(0, current_population - num_removed)
        pct_removed = 1.0*num_removed/current_population

        if spring_arrives is not None and current_date_obj >= spring_arrives:
            weather_adj = weather_adj_val
        if schools_closed_date is not None and current_date_obj >= schools_closed_date:
            schools_closed_adj = -0.3
        if business_closed_date is not None and current_date_obj >= business_closed_date:
            business_closed_adj = -0.35
        if stay_at_home_date is not None and current_date_obj >= stay_at_home_date:
            stay_at_home_adj = 0.5

        susceptible_net_quarantined = susceptible_pop - (susceptible_pop*stay_at_home_pct*stay_at_home_adj)

        pct_susceptible = 1.0*susceptible_net_quarantined/current_population

        rt = base_rt - weather_adj
        rt = rt*(1+(schools_closed_adj+business_closed_adj))
        if r_override is not None and r_override_date is not None:
            if current_date_obj >= r_override_date:
                rt = r_override


        new_infections = max(starting_infections, rt*new_infections*pct_susceptible)
        cases[current_date] = new_infections
        total_infected = min(population, total_infected+new_infections) # shouldn't this be current population?
        total_cases[current_date] = total_infected

        pct_infected_to_date = 1.0*total_infected/population # shouldn't this be current population?

        infection_date_obj = current_date_obj - timedelta(days= days_to_hospitalization)
        infection_date = infection_date_obj.strftime('%Y%m%d')
        if infection_date in cases:
            new_hospitalizations = cases[infection_date]*pct_require_bed
        else:
            new_hospitalizations = 0

        new_hospitalizations_by_date[current_date] = new_hospitalizations
        total_hospitalizations += new_hospitalizations
        total_hospitalization_by_date[current_date] = total_hospitalizations

        infection_date_obj = current_date_obj - timedelta(days= days_to_death)
        infection_date = infection_date_obj.strftime('%Y%m%d')
        if infection_date in cases:
            new_deaths = cases[infection_date]*fatality_rate
        else:
            new_deaths = 0
        deaths[current_date] = new_deaths


        total_deaths += new_deaths
        total_deaths_by_date[current_date] = total_deaths
        total_infected_living_by_date[current_date] = total_infected - total_deaths

        prev_date_obj = current_date_obj - timedelta(days=interval)
        prev_date = prev_date_obj.strftime('%Y%m%d')
        implied_doubling_days = interval*numpy.log(2)/numpy.log(cases[current_date]/cases[prev_date])
        for dt in [(current_date_obj - timedelta(days=d)) for d in range(interval-1,0,-1)]:
            dt_str = dt.strftime('%Y%m%d')
            prev_date = prev_date_obj.strftime('%Y%m%d')
            cases[dt_str] = cases[prev_date]*(2**(1.0/implied_doubling_days))
            total_cases[dt_str] = total_cases[prev_date]*(2**(1.0/implied_doubling_days))
            deaths[dt_str] = deaths[prev_date]*(2**(1.0/implied_doubling_days))
            prev_date_obj = dt

    cases_series = []
    sorted_series = sorted(cases.keys())
    for date in sorted_series[::interval]:
        cases_series.append({'date':date, 'val':cases[date]})
    deaths_series = []
    sorted_series = sorted(deaths.keys())
    for date in sorted_series[::4]:
        deaths_series.append({'date':date, 'val':deaths[date]})

    hospitalizations_series = []
    sorted_series = sorted(new_hospitalizations_by_date.keys())
    for date in sorted_series:
        hospitalizations_series.append({'date':date, 'val':new_hospitalizations_by_date[date]})

    return {'cases': cases_series, 'deaths':deaths_series, 'hospitalizations':hospitalizations_series}


def insert_api_data_to_db():
    url = 'https://covidtracking.com/api/states/daily'
    dbconnection = getDBConnection(cfg_dir + '/local-covid.ini')
    cursor = dbconnection.cursor()

    columns = ['date', 'state', 'positive', 'negative', 'pending', 'hospitalized', 'death',
                   'total', 'totalTestResults', 'fips', 'deathIncrease', 'hospitalizedIncrease', 'negativeIncrease',
                   'positiveIncrease', 'totalTestResultsIncrease']

    insert_query = 'INSERT INTO state_historical_data ('+ ','.join(columns) + ') VALUES (' + ','.join(['%s']*len(columns)) + ')'

    truth_data = {}
    with urllib.request.urlopen(url) as url_data:
        data = json.loads(url_data.read().decode())
        print(data)
        for entry in data:
            #print(entry)
            row_array = []
            for c in columns:
                try:
                    if c == 'date':
                        #datestr = datetime.datetime.strftime('%Y%m%d')
                        row_array.append(str(entry[c]))
                    else:
                        row_array.append(entry[c])
                except KeyError:
                    row_array.append(None)
            row = tuple(row_array)
            key = entry['state'] + str(entry['date'])
            truth_data[key] = row

    for k in truth_data:
        if 'NY' not in k:
            continue
        print(k + ": " + str(truth_data[k]))
    return

def main(argv):
    start_date = datetime.strptime('20200125', '%Y%m%d').date()
    override_date = datetime.strptime('20200415', '%Y%m%d').date()
    result_data = create_model('NY', None, 2.35, start_date, 2, 4, -0.4, 1.5, override_date)
    result_json = json.dumps(result_data)
    return result_json

if __name__ == '__main__':
    main(sys.argv[1:])