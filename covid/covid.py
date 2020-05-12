#!/usr/bin/python3
import logging
from astor_globals import *
from astor_square_utils import *
from marshmallow import Schema, fields
import json
import urllib.request
import datetime
import pytz
from build_truth_data import create_model, convert_truth_data_to_timeseries, get_ground_truth


def get_state_stats(state):
    query = "SELECT * FROM state_stats WHERE state_abbrev = %s"
    logging.debug("getting data from " + cfg_dir + '/' + env + '-covid.ini')
    dbconnection = getDBConnection(cfg_dir + '/' + env + '-covid.ini')
    state = State(state, dbconnection)
    return json.dumps(state.get_json())


def get_covid_parameters():
    query = "SELECT * FROM state_stats WHERE state_abbrev = %s"
    logging.debug("getting data from " + cfg_dir + '/' + env + '-covid.ini')
    dbconnection = getDBConnection(cfg_dir + '/' + env + '-covid.ini')
    covid_params = CovidParameters(dbconnection)
    return json.dumps(covid_params.get_json())


def get_covid_tracking_data():
    url = 'https://covidtracking.com/api/states'
    result = {}
    gmt = pytz.timezone('GMT')
    eastern = pytz.timezone('US/Eastern')
    with urllib.request.urlopen(url) as state_data:
        data = json.loads(state_data.read().decode())
        for entry in data:
            state = entry['state']
            positive = entry['positive'] if 'positive' in entry else 0
            date = datetime.datetime.strptime(entry['dateModified'], "%Y-%m-%dT%H:%M:%SZ") if 'dateModified' in entry else None
            dategmt = gmt.localize(date) if date is not None else None
            case_date = dategmt.astimezone(eastern).date() if dategmt is not None else None
            hospitalized = entry['hospitalized'] if 'hospitalized' in entry else 0
            death = entry['death'] if 'death' in entry and entry['death'] is not None else 0
            result[state] = {'positive': positive, 'date': case_date, 'hospitalized': hospitalized, 'deaths':death}
    return result


def update_covid_from_entry(covid, entry):
    covid.official_cases = entry['positive']
    covid.official_cases_date = entry['date']
    covid.official_deaths = entry['deaths']
    covid.official_deaths_date = entry['date']
    covid.hospitalized = entry['hospitalized']
    covid.hospitalized_date = entry['date']
    return


class CovidStates(object):

    def __init__(self, dbconnection=None, country=None):
        self.covid_states = []
        self.updated_date = None
        if country is None:
            self.query = "SELECT * from state_info"
        else:
            self.query = "SELECT * FROM state_info WHERE location_id LIKE '"+country+"%'"
        covid_tracking_data = get_covid_tracking_data()

        if dbconnection is not None:
            self.dbconnection = dbconnection
            cursor = dbconnection.cursor()
            cursor.execute(self.query)
            rows = cursor.fetchall()
            for row in rows:
                covid = Covid()
                self.create_covid_from_row(covid, row)
                if self.updated_date is None or covid.official_cases_date > datetime.datetime.strptime(self.updated_date, "%Y-%m-%d").date():
                    self.updated_date = covid.official_cases_date.strftime("%Y-%m-%d")

                try:
                    covid_tracking_entry = covid_tracking_data[covid.state_abbrev]
                    update_covid_from_entry(covid, covid_tracking_entry)
                    if (self.updated_date is None or covid.official_cases_date > datetime.datetime.strptime(self.updated_date, "%Y-%m-%d").date()):
                        self.updated_date = covid.official_cases_date.strftime("%Y-%m-%d")
                except KeyError as e:
                    logging.error('cannot find covid tracking data for ' + covid.state_abbrev)
                except Exception as e:
                    logging.error('miscellanesous error: ' + str(e))


                self.covid_states.append(covid)
        return

    def create_covid_from_row(self, covid, row):
        covid.location_id = row[0]
        covid.state_abbrev = row[1]
        covid.state_pop  = row[2]
        covid.num_hospitals  = row[3]
        covid.staffed_beds  = row[4]
        covid.official_deaths  = row[5]
        covid.official_deaths_date  = row[6]
        covid.official_cases  = row[7]
        covid.official_cases_date  = row[8]
        covid.state_name  = row[9]
        covid.state_hospital_detail_link  = row[10]
        covid.state_full_name  = row[11]
        return

    def get_json(self):
        result = {'date':self.updated_date, 'states':[c.get_json() for c in self.covid_states]}
        return result


class State(object):

    def __init__(self, state, dbconnection=None):
        self.state = state
        self.query = "SELECT * FROM state_stats WHERE state_abbrev = %s"
        self.dbconnection = dbconnection
        self.population = None
        self.stay_at_home_pct = None
        self.stay_at_home_date = None
        self.business_closed_date = None
        self.schools_closed_date = None
        self.pop_density = None
        self.pop_density_adj = None
        self.start_date_state = None
        self.spring_arrives = None
        self.num_hospitals = None
        self.staffed_beds = None
        self.pct_unusable_beds = None
        self.pct_require_bed = None
        self.days_to_hospitalization = None
        self.days_to_death = None
        self.fatality_rate = None
        self.median_hospital_stay = None
        if dbconnection is not None:
            self.build_object_from_query()

    def build_object_from_query(self):
        if self.dbconnection is None:
            return None
        cursor = self.dbconnection.cursor()
        cursor.execute(self.query, (self.state,))
        row = cursor.fetchone()
        if row is None:
            return None
        self.population = row[5]
        self.stay_at_home_pct = row[6]
        self.stay_at_home_date = row[7]
        self.business_closed_date = row[8]
        self.schools_closed_date = row[9]
        self.pop_density = row[11]
        self.pop_density_adj = row[12]
        self.start_date_state = row[13]
        self.spring_arrives = row[15]
        self.num_hospitals = row[16]
        self.staffed_beds = row[17]
        self.pct_unusable_beds = row[22]
        self.pct_require_bed = row[23]
        self.days_to_hospitalization = row[24]
        self.days_to_death = row[25]
        self.fatality_rate = row[26]
        self.median_hospital_stay = row[27]
        return True

    def set_dbconnection(self, dbconnection):
        self.dbconnection = dbconnection

    def get_json(self):
        schema = StateSchema()
        return schema.dump(self)

class Covid(object):
    def __init__(self):
        self.location_id = None
        self.state_abbrev = None
        self.state_name = None
        self.state_fill_name = None
        self.state_hospital_detail_link = None
        self.state_pop = None
        self.num_hospitals = None
        self.staffed_beds = None
        self.official_deaths = None
        self.official_deaths_date = None
        self.official_cases = None
        self.official_cases_date = None
        self.pct_unusable_beds = None
        self.pct_require_bed = None
        self.days_to_hospitalization = None
        self.days_to_death = None
        self.fatality_rate = None
        self.median_hospital_stay = None
        self.prev_double_rate_days = None
        self.double_rate_days = None
        self.prior_cases_deaths = None
        self.current_cases_deaths_method = None
        self.percent_tested_deaths_method = None
        self.current_cases_reported_case_method = None
        self.current_cases_est = None
        self.current_cases_percent_pop = None
        self.capacity_cases_total = None
        self.prior_to_capacity_cases = None
        self.capacity_cases = None
        self.peaking_cases_checksum = None
        self.cases_in_hospital_at_peaking = None
        self.days_till_hospitals_full = None
        self.beds_per_1000 = None
        self.beds_per_1000_formatted = None

        self.hospitalized = None
        self.hospitalized_date = None
        return

    def get_json(self):
        schema = CovidSchema()
        return schema.dump(self)


class CovidParameters(object):

    def __init__(self, dbconnection=None):

        self.query = "SELECT * FROM covid_parameters"
        self.dbconnection = dbconnection
        self.serial_interval = None
        self.r0_baseline = None
        self.pct_school_closing_impact = None
        self.pct_business_closing_impact = None
        self.fatality_rate = None
        self.pct_hospital_die = None
        self.days_to_hospital = None
        self.hospital_die_days = None
        self.hospital_live_days = None
        self.warm_weather_impact = None
        self.infection_start_date = None
        self.starting_infections = None
        self.r0_override = None
        self.r0_override_date = None
        if dbconnection is not None:
            self.build_object_from_query()

    def build_object_from_query(self):
        if self.dbconnection is None:
            return None
        cursor = self.dbconnection.cursor()
        cursor.execute(self.query,)
        row = cursor.fetchone()
        if row is None:
            return None
        self.serial_interval = row[0]
        self.r0_baseline = row[1]
        self.pct_school_closing_impact = row[2]
        self.pct_business_closing_impact = row[3]
        self.fatality_rate = row[4]
        self.pct_hospital_die = row[5]
        self.days_to_hospital = row[6]
        self.hospital_die_days = row[7]
        self.hospital_live_days = row[8]
        self.warm_weather_impact = row[9]
        self.infection_start_date = row[10]
        self.starting_infections = row[11]
        self.r0_override = row[12]
        self.r0_override_date = row[13]
        return True

    def get_json(self):
        schema = CovidParametersSchema()
        return schema.dump(self)


class StateSchema(Schema):
    population = fields.Int()
    stay_at_home_pct = fields.Float()
    stay_at_home_date = fields.Date()
    business_closed_date = fields.Date()
    schools_closed_date = fields.Date()
    pop_density = fields.Float()
    pop_density_adj = fields.Float()
    start_date_state = fields.Date()
    spring_arrives = fields.Date()
    num_hospitals = fields.Int()
    staffed_beds = fields.Int()
    pct_unusable_beds = fields.Float()
    pct_require_bed = fields.Float()
    days_to_hospitalization = fields.Float()
    days_to_death = fields.Float()
    fatality_rate = fields.Float()
    median_hospital_stay = fields.Float()


class CovidSchema(Schema):
    location_id = fields.Str()
    state_abbrev = fields.Str()
    state_name = fields.Str()
    state_fill_name = fields.Str()
    state_hospital_detail_link = fields.Str()
    state_pop = fields.Int()
    num_hospitals = fields.Int()
    staffed_beds = fields.Int()
    official_deaths = fields.Int()
    official_deaths_date = fields.Date()
    official_cases = fields.Int()
    official_cases_date = fields.Date()
    pct_unusable_beds = fields.Float()
    pct_require_bed = fields.Float()
    days_to_hospitalization = fields.Float()
    days_to_death = fields.Float()
    fatality_rate = fields.Float()
    median_hospital_stay = fields.Float()
    prev_double_rate_days = fields.Float()
    double_rate_days = fields.Float()
    prior_cases_deaths = fields.Float()
    current_cases_deaths_method = fields.Str()
    percent_tested_deaths_method = fields.Str()
    current_cases_reported_case_method = fields.Str()
    current_cases_est = fields.Float()
    current_cases_percent_pop = fields.Float()
    capacity_cases_total = fields.Float()
    prior_to_capacity_cases = fields.Float()
    capacity_cases = fields.Float()
    #peaking_cases_checksum = fields.Bool()
    cases_in_hospital_at_peaking = fields.Float()
    days_till_hospitals_full = fields.Float()
    beds_per_1000 = fields.Float()
    beds_per_1000_formatted = fields.Str()


class CovidParametersSchema(Schema):
    serial_interval = fields.Float()
    r0_baseline = fields.Float()
    pct_school_closing_impact = fields.Float()
    pct_business_closing_impact = fields.Float()
    fatality_rate  = fields.Float()
    pct_hospital_die = fields.Float()
    days_to_hospital = fields.Float()
    hospital_die_days  = fields.Float()
    hospital_live_days  = fields.Float()
    warm_weather_impact = fields.Float()
    infection_start_date = fields.Date()
    starting_infections = fields.Float()
    r0_override = fields.Float()
    r0_override_date = fields.Date()


def get_covid_data(country=None):
    env = None
    try:
        env = os.environ['ASTOR_ENV']
    except KeyError as e:
        try:
            if env is None:
                env = os.environ['NODE_ENV']
        except KeyError as e:
            pass

    if env is None:
        env = 'local'

    cfg_dir = None

    try:
        cfg_dir = os.environ['ASTOR_CFG_DIR']
    except KeyError as e:
        cfg_dir = None

    if cfg_dir is None:
        cfg_dir = '/usr/local/etc/astor_square/'

    try:
        api_cfg_dir = os.environ['ASTOR_API_CFG_DIR']
    except KeyError as e:
        api_cfg_dir = None

    if api_cfg_dir is None:
        api_cfg_dir = '/usr/local/etc/astor_square/'
    api_db_initfile = cfg_dir + '/' + env + '-api.ini'

    logging.debug("getting data from " + api_db_initfile)
    mapper = ConfigSectionMap(api_db_initfile)
    db_name = 'covid19'
    db_host = mapper.sectionMap('Database')['dbhost']
    user_name = mapper.sectionMap('Database')['user']
    password = mapper.sectionMap('Database')['password']
    dbconnection = psycopg2.connect("dbname='" + db_name + "' user='" + user_name + "' host='" + db_host + "'%s"
                                    % "password='" + password + "'")

    covid_states = CovidStates(dbconnection, country)
    return json.dumps(covid_states.get_json())

def get_state_timeline(state='NY', parameters={}):
    if state is None:
        state = 'NY'
    start_date = datetime.datetime.strptime('20200125', '%Y%m%d').date()
    override_date = datetime.datetime.strptime('20200415', '%Y%m%d').date()
    override_value = None
    # (state, start_pop, r0, start_date, starting_infections, interval, weather_adj_val, r_override, r_override_date):

    serial_interval = parameters['serial_interval'] if 'serial_interval' in parameters else 4
    r0_baseline = parameters['r0_baseline'] if 'r0_baseline' in parameters else 2.35
    start_date = datetime.datetime.strptime(parameters['infection_start_date'], '%Y-%m-%d').date() if 'infection_start_date' in parameters else datetime.datetime.strptime('20200125', '%Y%m%d').date()
    starting_cases = parameters['starting_infections'] if 'starting_infections' in parameters else 2

    override_value = parameters['r0_override'] if 'r0_override' in parameters else override_value
    override_date = datetime.datetime.strptime(parameters['r0_override_date'], '%Y-%m-%d').date() if 'r0_override_date' in parameters else override_date

    result_data = create_model(state, None, r0_baseline, start_date, starting_cases, serial_interval, override_value, override_date, parameters)
    result_json = json.dumps(result_data)
    return result_json



def get_historic_data(state):

    ground_truth = get_ground_truth(state)
    truth_timeseries = convert_truth_data_to_timeseries(ground_truth,1)
    try:
        result_json = json.dumps(truth_timeseries)
    except Exception as e:
        print('error: ' + str(e))
    return result_json
