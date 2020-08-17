# twitter_movie_ratings_challenge

Finds out the top movie generes rated by the users via twitter. From the movie titles and movie ratings
finds out the top movie generes for each year

## Overview

### Requirements
1. Install [Spark 2.4 or above](https://spark.apache.org/downloads.html)
1. Install [Python 3.0 or above](https://www.python.org/downloads/) 

### Usage
- `spark-submit movie_ratings.py`
- The solution accepts the following *optional* parameters
    - `-ratings_path` ratings.dat file path
    - `-movies_path` movies.dat file path
    - `-n` past number of years
    - `-o` final output path to save the results
- `spark-submit movie_ratings.py -ratings_path ./data/ratings.dat -movies_path ./data/movies.dat -n 10 -o output`

### Inputs
1. ratings.dat
    - text file with `::` as field delimiter
    - Fields
        - `user_id` 
        - `movie_id` will be the foreign key
        - `rating` will be an integer value between 0 and 10
        - `rating_timestamp` 
 
2. movies.dat
   - text file with `::` as field delimiter
   - Fields
        - `movie_id` primary key field
        - `movie_title` with the title will have year of the movie as well
        - `movie_gener` one or more generes separated by `|`, this could be empty as well

### Outputs
Output will be a CSV file order by year and gener 
- CSV format
- Fields
    - `movie_year` 
    - `gener`
 A sample output snippet
 <pre><code>
 jai-mac:twitter_movie_ratings_challenge jai$ head output/*.csv
movie_year,gener
2019,Talk-Show
2019,Short
2019,Reality-TV
2019,Western
2019,Documentary
2019,Crime
2019,Thriller
2019,News
2019,Action
 </code></pre>
### Assumptions
- Movie titles with no generes are not being listed

### ToDo
Add unit test coverage
        