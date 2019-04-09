#### Workload 1: Category and Trending Correlation
Overall, the implementation of workload 1 contains three files, which are responsible for mapper, combiner, and reducer respectively.
1. First of all, mappers read every single line from the original file, remove the information that is not necessary and generate kv pair (category, (video_id, country)).
2. In order to reduce the amount of data transfer among distributed system, combiner uses a set to remove duplicate data for each category, then combine all data with the same category into one line. Finally, the output format is like (category, (video_id, country_1, country_2, ... , country_n)).
3. Reducer is used to calculate the final result. For each category, I save all videos and the number of countries it appears into a dictionary, and then sum the number of countries, finally calculate the average number by sum divided by the number of videos in the category.

#### Workload 2: Controversial Trending Videos Identification
The spark processes are as below:
1. The program firstly read in the file, remove unnecessary columns and generate RDD pair ((video_id, country, category), (trending_date, likes, dislikes)).
2. Then, an aggregate function is applied, which includes sequencing function named 'merge()' and combiner function named 'combine()'. This step filters all records with the same key and only keeps two earliest records, since the requirement is to calculate the difference between first and second trending appearances. The output is like ((video_id, country, category), ({'date': date_1, 'difference': diff_1}, {'date': date_1, 'difference': diff_1})). The Value of RDD pair is tuple with two dictionaries inside, each dictionary represents a one of the two earliest trending records. Specifically, date is trending_date and difference = dislikes - likes.
3. Another map is used to calculate the difference in the growth of dislikes and likes. Back to the previous step, we already have difference = dislikes - likes for both record. Then what we need to do is calculate the result by result = difference_2 - difference_1. The RDD format is ((video_id, country, category), result).
4. Since the final output needs to be shown in descending order, an orderBy function is taken to sort them by the result. After sort, we only keep the top ten data and remove the rest of them.
5. To make the final output format is same with requirement, I use map function to reformat the data into the correct format (video_id, result, category, country).
6. Finally, write RDD into files.