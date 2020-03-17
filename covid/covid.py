#!/usr/bin/python3
from astor_globals import *
from astor_square_utils import *
from marshmallow import Schema, fields
import json

class CovidStates(object):

    def __init__(self, dbconnection=None):
        self.covid_states = []

        self.query = "SELECT * from state_info"
        if dbconnection is not None:
            self.dbconnection = dbconnection
            cursor = dbconnection.cursor()
            cursor.execute(self.query)
            rows = cursor.fetchall()
            for row in rows:
                covid = Covid()
                self.create_covid_from_row(covid, row)
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
        result = [c.get_json() for c in self.covid_states]
        return result

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
        return

    def get_json(self):
        schema = CovidSchema()
        return schema.dump(self)



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

def get_covid_data():
    mapper = ConfigSectionMap(api_db_initfile)
    db_name = 'covid19'
    db_host = mapper.sectionMap('Database')['dbhost']
    user_name = mapper.sectionMap('Database')['user']
    password = mapper.sectionMap('Database')['password']
    dbconnection = psycopg2.connect("dbname='" + db_name + "' user='" + user_name + "' host='" + db_host + "'%s"
                                    % "password='" + password + "'")

    covid_states = CovidStates(dbconnection)
    return json.dumps(covid_states.get_json())

