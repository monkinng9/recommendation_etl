from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import sqlalchemy
import pandas as pd

# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

# Function that return average ratings per course
def transform_avg_rating(rating_data):
    # Group by course_id and extract average rating per course
    avg_rating = rating_data.groupby('course_id').rating.mean()
    # Return sorted average ratings per course
    sort_rating = avg_rating.sort_values(ascending=False).reset_index()
    return sort_rating

# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

# The transformation should fill in the missing values
def transform_fill_programming_language(course_data):
    imputed = course_data.fillna({"programming_language": "R"})
    return imputed

def course_user_not_completed(user, courses, user_and_course, courses_data_not_null):
  # User complete all course
  user_allcourse = user.to_frame().merge(courses, how='cross')
  # Course that user not completed
  newdf = user_allcourse.merge(user_and_course,indicator = True, how='left').loc[lambda x : x['_merge']!='both']
  newdf = newdf[['user_id', 'course_id']]
  # Merge for programming language column
  newdf = newdf.merge(courses_data_not_null, on='course_id')
  newdf = newdf[['user_id', 'course_id', 'programming_language']].sort_values(by=['user_id'])
  return newdf

def transform_recommendations(user_prog_language, avg_course_ratings, courses_to_recommend):
  # Extract specific programming languages that user have learned.
  user_rec_course_with_lang = pd.merge(courses_to_recommend, user_prog_language,  how='inner', on=['user_id', 'programming_language'])
  # Merge 
  merged = user_rec_course_with_lang.merge(avg_course_ratings)
  # Sort values by rating and group by user_id
  grouped = merged.sort_values("rating", ascending=False).groupby("user_id")
  # Produce the top 3 values and sort by user_id
  recommendations = grouped.head(3).sort_values("user_id").reset_index()
  # Return top 3 recommendations
  final_recommendations = recommendations[["user_id", "course_id", 'programming_language',"rating"]]
  return(final_recommendations)

def etl():
  # Complete the connection URI
  connection_uri = "postgresql://postgres:mamalala@datacamp-demo.cijubbxfljwq.ap-southeast-1.rds.amazonaws.com:5432/datacamp_application" 
  db_engine = sqlalchemy.create_engine(connection_uri)

  # Extract the rating data into a DataFrame    
  rating_data = extract_table_to_pandas('rating',db_engine)
  # Extract user id
  user = rating_data['user_id'].drop_duplicates()
  # Extract user id with course
  user_and_course = rating_data[['user_id', 'course_id']]

  # Use transform_avg_rating on the extracted data and print results
  avg_rating_data = transform_avg_rating(rating_data)

  # Extract the course data into a DataFrame    
  courses_data = extract_table_to_pandas('courses',db_engine)

  # Prin

  courses_data_not_null = transform_fill_programming_language(courses_data)
  courses = courses_data_not_null[['course_id']]
  # Print out the number of missing values per column of transformed

  # Course's programming language
  user_course_language = rating_data.merge(courses_data_not_null, on='course_id')
  user_course_language = user_course_language[['user_id', 'course_id', 'programming_language']]

  # Course recommend for user
  courses_to_recommend = course_user_not_completed(user, courses, user_and_course, courses_data_not_null)

  # The programming language that the user learns in the courses
  user_prog_language = user_course_language[['user_id','programming_language']].drop_duplicates()
  
  final_recommendations = transform_recommendations(user_prog_language, avg_rating_data, courses_to_recommend)

  final_recommendations.to_sql('Recommendation', db_engine, if_exists="replace")
  print("Completed!")


def greeting():
  print( "Greetings!")

default_args = {
  'owner': 'chayut',
  'retires': 5,
  'retry_delay': timedelta(minutes=5)
}

with DAG(
  default_args=default_args,
  dag_id='Recommendation_etlV3',
  description='Recommedation using python operator',
  start_date=datetime(2022, 11, 22),
  schedule_interval='@daily'
) as dag:
  task1 = PythonOperator(
    task_id = 'etl',
    python_callable = etl
  )

  task1