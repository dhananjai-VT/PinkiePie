from feature_query_PPie_no_hagrid_Mar14 import *

import pandas as pd
import numpy as np
import pickle
import sqlalchemy as sa
import datetime as dtm
import credstash

from sklearn.externals import joblib
from sklearn.ensemble import GradientBoostingClassifier

# get redshift credentials
password = credstash.getSecret("pr::redshift::engineer_password", region='us-west-2')

### GLOBAL VARS ###
connstr = 'redshift+psycopg2://engineer:{}@172.31.13.196:5439/varsitytutors'.format(password)
engine = sa.create_engine(connstr, connect_args={'sslmode': 'verify-ca'})

#load call data
with engine.connect() as conn, conn.begin():
   df_master = pd.read_sql(sa.text(feature_query), conn)
   
df=df_master.copy()

# cleansing dataframe and actions tracker data for iphone leads
if (len(df)==df.contact_id.value_counts().sum()):
    print("All good")
else:
    print("Duplicate records exist")
print(len(df))
df.date_call = pd.to_datetime(df.date_call)
df.drop_duplicates(inplace=True)
df.date_contact = pd.to_datetime(df.date_contact)
print(len(df))

#removing nulls
values={'latitude':-200.0
       , 'longitude':-200.0
       , 'hrs_response_weblead':-1.0
       , 'tags_name':'No Data'
       , 'lead_source_roll_up':'No Data'
       , 'total_connects':0
       , 'total_inbounds':0
       , 'total_talktime_mins':0          
       ,'last_call_cc90':0
       ,'frequency_cc90s':-1
       ,'total_talktime_mins':0
       ,'prior_cc90s':0
       ,'avg_talktime':-1
       ,'last_cc90_timestamp':pd.Timestamp(year=1900, month=1, day=1, hour=12)
       }

df.fillna(value=values,inplace=True)

#Adding features
df=df.assign(lead_age = (pd.Timestamp.now(tz='America/Chicago').tz_localize(None)-df.date_contact).dt.days*24 )
df = df.assign(
                percent_cc90s = np.where(df.previous_attempts>0,df.prior_cc90s/df.attempt_ord,0)
               ,percent_connect = np.where(df.previous_attempts>0,df.total_connects/df.previous_attempts,0)
               ,percent_inbounds = np.where(df.previous_attempts>0,df.total_inbounds/df.previous_attempts,0)
               ,last_call_cc90 = df.cc90
               ,avg_talktime =np.where(df.lead_age>0,df.total_talktime_mins/(df.lead_age/24),-1) 
               ,days_since_last_cc90 = (pd.Timestamp.now(tz='America/Chicago').tz_localize(None)-df.last_cc90_timestamp).dt.days
               ,hr_call_made = pd.Timestamp.now(tz='America/Chicago').tz_localize(None).hour
               ,dow_call_made = pd.Timestamp.now(tz='America/Chicago').tz_localize(None).weekday() 
               ,mon_contact_created = lambda v: v['date_contact'].dt.month
               ,week_contact_created = lambda v: v['date_contact'].dt.week
                )

#Removing any leftover nulls
values={'latitude':-200.0
       , 'longitude':-200.0
       , 'hrs_response_weblead':-1.0
       , 'tags_name':'No Data'
       , 'lead_source_roll_up':'No Data'
       , 'total_connects':0
       , 'total_inbounds':0
       , 'total_talktime_mins':0          
       ,'last_call_cc90':0
       ,'frequency_cc90s':-1
       ,'total_talktime_mins':0
       ,'prior_cc90s':0
       ,'avg_talktime':-1
       ,'last_cc90_timestamp':pd.Timestamp(year=1900, month=1, day=1, hour=12)
       }
df.fillna(value=values,inplace=True)

#top features
top_features = [
    
    #'call_id'
    # lead attributes
     'latitude'
    , 'longitude'
    #, 'is_hagrid_self'
    #, 'is_hagrid_child'
    #, 'is_hagrid_else'
    #, 'is_hagrid_now'
    #, 'is_hagrid_later'
    #, 'is_hagrid_unsure'
    , 'is_test_prep'
    , 'lead_source_roll_up'
    , 'is_phone_lead'
    , 'dow_contact_created'
    , 'hr_contact_created'
    , 'mon_contact_created'
    , 'week_contact_created'
    
#call attributes
    
    #, 'date_call'
    , 'hr_call_made'
    #, 'day_call_made'
    , 'dow_call_made'
    #, 'week_call_made'
    #, 'hrs_response_weblead'
    #, 'tags_name'
    #, 'created_internally'
    #, 'requested_specific_tutor'
    #, 'is_email_id_given'
    , 'previous_attempts'
    , 'prior_cc90s'
    , 'avg_talktime'
    , 'percent_inbounds'
    , 'percent_connect'
    , 'lead_age'
    , 'days_since_last_cc90'
    #, 'hrs_since_last_cc90'
    , 'last_call_cc90'
    #, 'total_connects'
    #, 'total_inbounds'
    #, 'total_talktime_mins'
    #, 'frequency_cc90s'
    #, 'talk_duration_mins'
    #, 'percent_cc90s'
    #, 'frequency_connects'
    #, 'frequency_inbounds'
    # labels
    , 'contact_id'
    , 'date_contact'
    , 'cc90'
    , 'closed_that_call'
    , 'closed_ever'   
]

set1 = set(df.columns)
set2 = set(top_features)
droppable_features = set1.difference(set2)
df = df.drop(columns=droppable_features,axis=1)

#creating dummies for 1-0 encoding
prefixes={'lead_source_roll_up':'is_lead_source_roll_up'}
df = pd.get_dummies(data=df, columns=['lead_source_roll_up'], drop_first=True, prefix=prefixes)

#Score generation

# load model and dimensions
model = joblib.load('1year_sql_prod-PPie-no_hagrid-Mar14.pkl')
with open('dims_1year_sql_prod-PPie-no_hagrid-Mar14.pkl', 'rb') as f:
    dimensions = pickle.load(f)

droppable_columns=['cc90', 'closed_that_call', 'closed_ever' ]

#create X by dropping target variable
X = df.drop(columns=droppable_columns, axis=1)

# store missing dimensions and extra dimensions
d = set(dimensions)
d2 = set(X.columns)
missing_dims = d.difference(d2)
extra_dims = d2.difference(d)

# remove extra columns, add missing columns, align column names
X = X.drop(columns=extra_dims)
d = dict.fromkeys(missing_dims, 0)
X = X.assign(**d)
X.sort_index(axis=1, inplace=True)

#copy dataframe to rank predicted probabilties
score_df = X.copy()
score_df['prob_cc90'] = model.predict_proba(score_df.drop(['contact_id', 'date_contact'],axis=1))[:,-1]
score_df = score_df.sort_values(by='prob_cc90', ascending=True)
score_df['score_final'] = np.around(score_df.prob_cc90, decimals=5)

generated_time = dtm.datetime.utcnow()
write_timestamp = str(generated_time.time())[0:8].replace(':', '')
write_name = 'PinkiePie_scores_' + write_timestamp + '.json'
score_df[['contact_id','score_final']].to_json(write_name , orient='records')




















