# Advance-Database-Methods-assignment
MapReduce and SparkScala assignments 

For this exercise, you are tasked with writing your own Hadoop MapReduce program in Python and to
run it on the cluster on the provided datasets.   
You may look at the exercise sheet for all the information on the datasets and this task.

For each garment group, show the most frequent product, the second most frequent section and the most frequent department it appears inside the article.csv file; make sure output has the following schema:

            garment_group_name, prod_name, section_name,  department_name

The product names are stored in "prod_name", the deparment name in "department_name", the garment group in "garment_group_name" and the section in "section_name". In case that there are multiple departments, garment groups or sections with the same number of occurences, you may resolve these conflicts randomly, i.e. pick one of them arbitrarily. In case there is only one section, or all sections appear with the same frequency, just pick the most frequent one, and resolve conflicts randomly. 

Make sure that your program correctly deals with the header, and possible sparse values.

Write a MapReduce job with all three datasets as input and following output:**  
for all customers with 'fashion\_news\_frequency' = 'Regularly', show the number of transactions they appear in where the article  has a 'graphical\_appearance\_name' equal to 'Solid' and a 'colour\_group\_name' equal to 'Light Beige'


Make sure to have the following format in your final output:

            customer_id,count_transactions
