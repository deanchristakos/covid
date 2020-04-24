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
us_population = 327200000
yearly_deaths = 2813503
yearly_births = 3791712

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


def convert_truth_data_to_timeseries(truth_data, interval=1):

    hospitalizations = []
    deaths = []
    positives = []
    start = True
    truth_data_keys = sorted(truth_data.keys())
    if interval == 1:
        for i in range(0, len(truth_data_keys)):
            dt = truth_data_keys[i][2:]
            k = truth_data_keys[i]
            try:
                hospitalizations.append( {'date':dt, 'val': truth_data[k]['hospitalizedIncrease']} )
                deaths.append({'date': dt, 'val': truth_data[k]['deathIncrease']})
                positives.append({'date': dt, 'val': truth_data[k]['positiveIncrease']})
            except Exception as e:
                error = str(e)
                pass
    else:
        for i in range(0, len(truth_data_keys), interval):
            dt = truth_data_keys[i][2:]
            if (i > 0):
                daily_keys = [k for k in truth_data_keys[i-interval+1:i]]
                hospitalizations_sum = int(numpy.sum([truth_data[k]['hospitalizedIncrease'] for k in truth_data_keys[i-interval+1:i]]))
                hospitalizations.append({'date': dt, 'val': hospitalizations_sum})
                deaths_sum = int(numpy.sum([truth_data[k]['deathIncrease'] for k in truth_data_keys[i-interval+1:i]]))
                deaths.append({'date': dt, 'val': deaths_sum})
                positives_sum = int(numpy.sum([truth_data[k]['positiveIncrease'] for k in truth_data_keys[i-interval+1:i]]))
                positives.append({'date': dt, 'val': positives_sum})
            else:
                key = truth_data_keys[i]
                hospitalizations.append({'date': dt, 'val': truth_data[key]['hospitalizedIncrease']})
                deaths.append({'date': dt, 'val': truth_data[key]['deathIncrease']})
                positives.append({'date': dt, 'val': truth_data[key]['positiveIncrease']})

    """
    for k in sorted(truth_data.keys())[::interval]:
        dt = k[2:]
        hospitalizations.append( {'date':dt, 'val': truth_data[k]['hospitalizedIncrease'] } )
        deaths.append( { 'date':dt, 'val': truth_data[k]['deathIncrease'] })
        positives.append( { 'date':dt, 'val': truth_data[k]['positiveIncrease'] } )"""

    return { 'actual_hospitalizations': hospitalizations, 'actual_deaths': deaths, 'actual_positives': positives }


def create_model(state, start_pop, r0, start_date, starting_infections, interval, weather_adj_val, r_override, r_override_date, parameters={}):
    num_removed = 0
    query = "SELECT * FROM state_stats WHERE state_abbrev = %s"
    dbconnection = getDBConnection(cfg_dir + '/' + env + '-covid.ini')
    cursor = dbconnection.cursor()
    cursor.execute(query, (state,))
    row = cursor.fetchone()
    if row is None:
        return None

    pct_require_bed = parameters['pct_require_bed'] if 'pct_require_bed' in parameters else .0125
    days_to_hospitalization = parameters['days_to_hospital'] if 'days_to_hospital' in parameters else 12.1
    days_to_death = parameters['days_to_death'] if 'days_to_death' in parameters else 23.6
    fatality_rate = parameters['fatality_rate'] if 'fatality_rate' in parameters else .0066
    #median_hospital_stay = parameters['population'] if 'population' in parameters else 15.0
    prev_double_rate_days = 5.0
    double_rate_days = 5.0
    stay_at_home_pct = parameters['stay_at_home_pct'] if 'stay_at_home_pct' in parameters else 1

    population = parameters['population'] if 'population' in parameters else row[5]
    stay_at_home_pct = parameters['stay_at_home_pct'] if 'stay_at_home_pct' in parameters else row[6]
    stay_at_home_date = datetime.strptime(parameters['stay_at_home_date'], '%Y-%m-%d').date() if 'stay_at_home_date' in parameters else row[7]
    business_closed_date = datetime.strptime(parameters['business_closed_date'], '%Y-%m-%d').date() if 'business_closed_date' in parameters else row[8]
    schools_closed_date = datetime.strptime(parameters['schools_closed_date'], '%Y-%m-%d').date() if 'schools_closed_date' in parameters else row[9]
    pop_density_adj = parameters['pop_density_adj'] if 'pop_density_adj' in parameters else row[12]
    start_date_state = datetime.strptime(parameters['start_date_state'], '%Y-%m-%d').date() if 'start_date_state' in parameters else row[13]
    spring_arrives = datetime.strptime(parameters['spring_arrives'], '%Y-%m-%d').date() if 'spring_arrives' in parameters else row[15]
    num_hospitals = parameters['num_hospitals'] if 'num_hospitals' in parameters else row[16]
    staffed_beds = parameters['staffed_beds'] if 'staffed_beds' in parameters else row[17]
    pct_unusable_beds = parameters['pct_unusable_beds'] if 'pct_unusable_beds' in parameters else row[22]
    pct_require_bed = parameters['pct_require_bed'] if 'pct_require_bed' in parameters else row[23]
    days_to_hospitalization = parameters['days_to_hospital'] if 'days_to_hospital' in parameters else row[24]
    days_to_death = parameters['days_to_death'] if 'days_to_death' in parameters else row[25]
    fatality_rate = parameters['fatality_rate'] if 'fatality_rate' in parameters else row[26]

    deaths_per_day = yearly_deaths/365

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
    new_hospitalizations_by_date[current_date] = 0
    deaths[current_date] = 0
    total_deaths_by_date[current_date] = 0

    for d in range(0,30):

        for v in range(0,interval):
            btw_date_obj = current_date_obj + timedelta(days=v)
            btw_date = btw_date_obj.strftime('%Y%m%d')
            cases[btw_date] = cases[current_date]
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
            new_hospitalizations_by_date[dt_str] = new_hospitalizations_by_date[prev_date]*(2**(1.0/implied_doubling_days))
            prev_date_obj = dt

    cases_series = []
    sorted_series = sorted(cases.keys())
    for date in sorted_series[::interval]:
        cases_series.append({'date':date, 'val':cases[date]})
    cases_series = interpolate(cases_series, interval)

    deaths_series = []
    sorted_series = sorted(deaths.keys())
    for date in sorted_series[::interval]:
        deaths_series.append({'date':date, 'val':deaths[date]})

    deaths_series = interpolate(deaths_series, interval)
    daily_deaths_series = []
    """
    last_death_entry = None
    for d in deaths_series[1:]:
        new_deaths = d['val']
        incremental_deaths = new_deaths/interval
        date_obj = datetime.strptime(d['date'], '%Y%m%d').date() - timedelta(days=interval-1)
        prev_date_obj = date_obj - timedelta(days=1)
        prev_date_str = prev_date_obj.strftime('%Y%m%d')
        prev_deaths = deaths[prev_date_str]
        slope = (new_deaths - prev_deaths)/2
        if len(daily_deaths_series) == 0:
            immediate_prev_deaths = 0
        else:
            immediate_prev_deaths = daily_deaths_series[-1]['val']
        starting_point = immediate_prev_deaths + slope
        area_sum = 0.5*slope
        increase_series = calc_incremental_increase(new_deaths, slope, interval, area_sum)
        for i in range(0, interval):
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime('%Y%m%d')
            try:
                daily_deaths_series.append({'date':date_str, 'val': increase_series[i]})
            except Exception as e:
                print('problem: '+ str(e))
    deaths_series = sorted(daily_deaths_series, key=lambda x: x['date'])"""


    hospitalizations_series = []
    sorted_series = sorted(new_hospitalizations_by_date.keys())
    for date in sorted_series[::4]:
        hospitalizations_series.append({'date':date, 'val':new_hospitalizations_by_date[date]})

    hospitalizations_series = interpolate(hospitalizations_series, interval)
    return {'cases': cases_series, 'deaths':deaths_series, 'hospitalizations':hospitalizations_series}


def calc_change(start, slope, interval):
    nums = []
    val = start
    for i in range(0, interval):
        val = val + slope
        nums.append(val)
    if nums[-1] < 0:
        # we have to change this and use a different slope if this is less than 0
        slope = (0 - 1.0*start) / interval
        if (slope*interval) >= 0:
            nums = calc_change(start, slope, interval)
        else:
            nums = [n if n >=0 else 0 for n in nums]
    return nums

def calc_incremental_increase(total, slope, interval, starting_point=0, area_sum = 0):
    prev_area = 0
    areas = []

    reverse = False
    if slope < 0:
        slope = -slope
        reverse = True
    for i in range(int(starting_point),int(interval)+int(starting_point)):
        area = 0.5*slope*(i+1)*(i+1) - area_sum
        areas.append(area)
        area_sum += area
        prev_area = area
    if reverse:
        areas = areas[::-1]
    return areas[1:]

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


def calculate_final_val(area, start, interval):

    result = 2*area/interval +start
    return result

    num = area - ( float(start) / (2*(interval))  )
    denom = ( (interval+1)/2.0 - 1.0/( 2*(interval) ) )
    final = num/denom
    return final


def calculate_slope_old(start, final, interval):
    return (1.0*final - start)/(interval)

def calculate_slope(start, total, interval):
    sum = 0
    for i in range(1, interval+1):
        sum += i
    slope = (total - 4*start)/sum
    return slope


def interpolate(dated_data, interval):
    starting_date = datetime.strptime(dated_data[0]['date'], '%Y%m%d').date()
    current_date = starting_date
    interpolated_dated_values = [dated_data[0]]
    interpolated_index = interval
    for i in range(1, len(dated_data)):
        prev_entry = dated_data[i - 1]
        prev_date = prev_entry['date']
        prev_val = prev_entry['val']
        entry = dated_data[i]
        dt = entry['date']
        val = entry['val']

        prev_interpolated_val = interpolated_dated_values[interpolated_index - interval]['val']
        slope = calculate_slope(prev_interpolated_val, val, interval)

        interpolation = calc_change(prev_interpolated_val, slope, interval)
        interp_sum = numpy.sum(interpolation)

        prev_date_obj = datetime.strptime(prev_date, '%Y%m%d').date()
        interp_date = prev_date_obj + timedelta(days=1)
        for i in range(0, len(interpolation)):
            interpolated_dated_values.append({'val': interpolation[i], 'date': interp_date.strftime('%Y%m%d')})
            interp_date = interp_date + timedelta(days=1)

        interpolated_index += interval
    return interpolated_dated_values

def experiment_with_data(data = [0, 1, 5, 10, 20, 30, 35, 30, 20, 10, 5, 1, 0]):
    #data = [0, 1, 5, 10, 20, 30, 35, 30, 20, 10, 5, 1, 0]
    dated_values = []
    starting_date = datetime.strptime('20200301', '%Y%m%d').date()
    current_date = starting_date
    interval = 4
    for i in range(0, len(data)):
        dated_values.append({'date': current_date.strftime('%Y%m%d'), 'val':data[i]})
        current_date = current_date + timedelta(days=interval)
    print('dated values: ' + str(dated_values))
    interpolated_dated_values = [ dated_values[0] ]
    interpolated_index = 4
    prev_area = 0
    for i in range(1, len(dated_values)):
        prev_entry = dated_values[i-1]
        prev_date = prev_entry['date']
        prev_val = prev_entry['val']
        entry = dated_values[i]
        dt = entry['date']
        val = entry['val']

        prev_interpolated_val = interpolated_dated_values[ interpolated_index - interval]['val']

        slope = calculate_slope(prev_interpolated_val, val, interval)

        interpolation = calc_change(prev_interpolated_val, slope, interval)
        interp_sum = numpy.sum(interpolation)

        prev_date_obj = datetime.strptime(prev_date, '%Y%m%d').date()
        interp_date = prev_date_obj + timedelta(days=1)
        for i in range(0,len(interpolation)):
            interpolated_dated_values.append({'val': interpolation[i], 'date':interp_date})
            interp_date = interp_date + timedelta(days=1)

        interpolated_index += interval
        pass
    return interpolated_dated_values


def main(argv):
    #calc_incremental_increase(100,6.25,4, 1, 6.25)
    interpolated_dated_values = experiment_with_data()
    sys.exit(0)
    start_date = datetime.strptime('20200125', '%Y%m%d').date()

    ground_truth = get_ground_truth('NY')
    truth_timeseries = convert_truth_data_to_timeseries(ground_truth, 4)
    override_date = datetime.strptime('20200415', '%Y%m%d').date()
    result_data = create_model('NY', None, 2.35, start_date, 2, 4, -0.4, 1.5, override_date)
    result_json = json.dumps(result_data)
    return result_json

if __name__ == '__main__':
    main(sys.argv[1:])